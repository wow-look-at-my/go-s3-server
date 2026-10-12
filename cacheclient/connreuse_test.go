package cacheclient

import (
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// countConns makes srv count each connection it accepts.
func countConns(srv *httptest.Server) *atomic.Int64 {
	var conns atomic.Int64
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	return &conns
}

// A TLS endpoint must carry every request of a build on one HTTP/2
// connection. Each extra connection is a full TLS handshake on the server.
func TestWebBackend_HTTP2SharesOneConnection(t *testing.T) {
	var protos sync.Map
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protos.Store(r.Proto, true)
		http.Error(w, "no such key", http.StatusNotFound)
	}))
	srv.EnableHTTP2 = true
	conns := countConns(srv)
	srv.StartTLS()
	defer srv.Close()

	b := testBackend(t, srv.URL)
	defer b.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	b.client.Transport.(*http.Transport).TLSClientConfig.RootCAs = pool

	const requests = 32
	var wg sync.WaitGroup
	for idx := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("%064x", idx)
			r := b.getIndividual(id, "cache/"+id, hashOf(id))
			require.True(t, r.miss)
		}()
	}
	wg.Wait()

	_, h2 := protos.Load("HTTP/2.0")
	require.True(t, h2, "the client must negotiate HTTP/2 with a TLS server that offers it")
	_, h1 := protos.Load("HTTP/1.1")
	require.False(t, h1, "no request may fall back to HTTP/1.1 when HTTP/2 was offered")
	require.EqualValues(t, 1, conns.Load(),
		"concurrent requests must share one HTTP/2 connection, not open one handshake each")
}

// An HTTP/1.1 connection goes back to the pool only when its body was read to
// EOF. A miss, an error status and a chunked batch stream must all leave it
// reusable, or each request after them pays a new handshake.
func TestWebBackend_HTTP1ReusesConnectionAfterEveryAnswer(t *testing.T) {
	var batches atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/_batch/get"):
			// Alternate an all-miss stream with an error status. The stream is
			// flushed in halves so it goes out chunked, as a real one does.
			if batches.Add(1)%2 == 0 {
				http.Error(w, strings.Repeat("x", 4096), http.StatusInternalServerError)
				return
			}
			body := buildBatchTar(t, batchGetManifest{}, nil)
			w.WriteHeader(http.StatusOK)
			w.Write(body[:len(body)/2])
			w.(http.Flusher).Flush()
			w.Write(body[len(body)/2:])
		default:
			http.Error(w, strings.Repeat("x", 4096), http.StatusNotFound)
		}
	}))
	conns := countConns(srv)
	srv.Start()
	defer srv.Close()

	b := testBackend(t, srv.URL)
	defer b.Close()
	b.maxRetries = 0

	for idx := range 10 {
		id := fmt.Sprintf("%064x", idx)
		r := b.getIndividual(id, "cache/"+id, hashOf(id))
		require.True(t, r.miss)
		_, _, _, _, miss, _, err := b.getBatchTest(id, "cache/"+id)
		require.NoError(t, err)
		require.True(t, miss)
	}
	require.EqualValues(t, 1, conns.Load(),
		"sequential requests must reuse one connection; each extra one is a handshake")
}
