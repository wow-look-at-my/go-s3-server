package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A server that gives no answer is asked once. Later requests are a miss at
// once, a single probe per downProbeEvery still goes out, and the first answer
// brings the server back.
func TestServerDownSkipsRequestsUntilItAnswers(t *testing.T) {
	t.Serial() // stallTimeout and downProbeEvery are package state
	oldStall, oldProbe := stallTimeout, downProbeEvery
	stallTimeout, downProbeEvery = 150*time.Millisecond, 300*time.Millisecond
	defer func() { stallTimeout, downProbeEvery = oldStall, oldProbe }()

	var hits atomic.Int32
	var answering atomic.Bool
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !answering.Load() {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	b := testBackend(t, srv.URL)
	defer b.Close()
	ask := func() error {
		req, err := http.NewRequest("GET", srv.URL+"/x", nil)
		require.NoError(t, err)
		resp, err := b.doRetryGET(req)
		if err == nil {
			closeBody(resp.Body)
		}
		return err
	}

	start := time.Now()
	require.Error(t, ask())
	require.Less(t, time.Since(start), 2*time.Second, "a request with no answer is not retried")
	require.Equal(t, int32(1), hits.Load())

	start = time.Now()
	for range 20 {
		require.ErrorIs(t, ask(), errServerDown)
	}
	require.Less(t, time.Since(start), 100*time.Millisecond, "a down server costs a lookup nothing")
	require.Equal(t, int32(1), hits.Load(), "nothing goes out before the probe is due")
	require.Equal(t, uint32(20), b.SkippedDown.Load())

	answering.Store(true)
	require.Eventually(t, func() bool { return ask() == nil }, 5*time.Second, 20*time.Millisecond)
	require.Zero(t, b.downSince.Load(), "an answer brings the server back")
	require.NoError(t, ask())
}

// A lookup the build waits on that gets a slow answer takes the answer, and
// marks the server down. A look-ahead request with the same delay does not.
func TestSlowAnswerToALookupMarksTheServerDown(t *testing.T) {
	t.Serial() // slowAnswer and downProbeEvery are package state
	oldSlow, oldProbe := slowAnswer, downProbeEvery
	slowAnswer, downProbeEvery = 100*time.Millisecond, time.Hour
	defer func() { slowAnswer, downProbeEvery = oldSlow, oldProbe }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b := testBackend(t, srv.URL)
	defer b.Close()
	ask := func(kind string) error {
		req, err := http.NewRequest("POST", srv.URL+"/x", nil)
		require.NoError(t, err)
		req.Header.Set(HeaderKind, kind)
		resp, err := b.doRetryGET(req)
		if err == nil {
			closeBody(resp.Body)
		}
		return err
	}

	require.NoError(t, ask(KindLookAhead))
	require.Zero(t, b.downSince.Load(), "nobody waits on a look-ahead request")
	require.NoError(t, ask(KindCritical), "the slow answer is still used")
	require.NotZero(t, b.downSince.Load())
	require.ErrorIs(t, ask(KindCritical), errServerDown)
}
