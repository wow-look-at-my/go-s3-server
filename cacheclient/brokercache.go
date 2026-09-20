// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// brokerCache is the cache in a process that did not open the directory. It
// holds no DiskCache, writes no index entry and trims nothing. Every read and
// write goes to the owner over the socket, and what comes back is the identity
// of a file the owner has already written.
//
// OutputFile still answers a path, because that is what the contract promises
// and what a compiler opens. The layout is the owner's, and this process
// computes the same name from the same directory.
type brokerCache struct {
	link *brokerLink
	dir  string
}

// errBrokerMiss is why a broker answer is not an entry: the owner reported no
// such action, or answered something this process cannot read.
var errBrokerMiss = errors.New("the build cache owner has no such entry")

// brokerIdleConns is how many sockets a child keeps warm. A go command compiles
// in parallel, so its cache reads are parallel, and a pool of one serializes
// them behind each other.
const brokerIdleConns = 64

// brokerDialWait bounds the connect. An owner on this machine accepts at once,
// so a wait past this is a socket whose owner is gone.
const brokerDialWait = 2 * time.Second

// brokerLink is the socket, and every call on it blocks in a read until the
// answer arrives. Nothing polls.
type brokerLink struct {
	client *http.Client
}

func newBrokerLink(path string) *brokerLink {
	dialer := &net.Dialer{Timeout: brokerDialWait}
	return &brokerLink{client: &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", path)
			},
			MaxIdleConns:        brokerIdleConns,
			MaxIdleConnsPerHost: brokerIdleConns,
			MaxConnsPerHost:     brokerIdleConns,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}}
}

// dialBroker answers the cache this process should use when a live owner is
// named in the environment. A socket file outlives the process that made it,
// so the ping is what decides, and it also reports the directory the owner
// writes into.
func dialBroker() Cache {
	path := os.Getenv(brokerEnv)
	if path == "" || os.Getenv(brokerOffEnv) != "" {
		return nil
	}
	link := newBrokerLink(path)
	resp, err := link.client.Get("http://broker" + brokerPing)
	if err != nil {
		return nil
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	dir := resp.Header.Get(dirHeader)
	if resp.StatusCode != http.StatusOK || dir == "" {
		return nil
	}
	brokerNotice("cache: served by the build cache owner at %s", path)
	return &brokerCache{link: link, dir: dir}
}

// Get asks the owner for an action's output. A miss answers the error the
// contract names, exactly as a local miss does.
func (c *brokerCache) Get(id ActionID) (Entry, error) {
	resp, err := c.link.client.Get("http://broker" + brokerEntry + "?id=" + hex.EncodeToString(id[:]))
	if err != nil {
		return Entry{}, &entryNotFoundError{Err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return Entry{}, &entryNotFoundError{Err: errBrokerMiss}
	}
	return readEntry(resp.Header)
}

// Put hands a body to the owner by naming the file it sits in. The owner reads
// it before it answers, so this process may exit as soon as Put returns.
//
// A caller that already holds an open file names that file. One that holds
// anything else spills to a temporary file first: the bytes have to reach
// another process, and a path is what this protocol carries.
func (c *brokerCache) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	path, cleanup, err := readerPath(file)
	if err != nil {
		return OutputID{}, 0, err
	}
	defer cleanup()
	query := "?id=" + hex.EncodeToString(id[:]) + "&path=" + urlValue(path)
	resp, err := c.link.client.Post("http://broker"+brokerPut+query, "application/octet-stream", nil)
	if err != nil {
		return OutputID{}, 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return OutputID{}, 0, fmt.Errorf("cache put: the build cache owner answered %s", resp.Status)
	}
	entry, err := readEntry(resp.Header)
	return entry.OutputID, entry.Size, err
}

// OutputFile answers where the owner stored an output. The layout belongs to
// DiskCache, and this is the same name it writes.
func (c *brokerCache) OutputFile(out OutputID) string {
	return filepath.Join(c.dir, fmt.Sprintf("%02x", out[0]), fmt.Sprintf("%x", out)+"-d")
}

// FuzzDir is the owner's, like everything else in the directory.
func (c *brokerCache) FuzzDir() string {
	return filepath.Join(c.dir, "fuzz")
}

// Close drops the sockets. Nothing here owns a file, an upload or a trim, so
// there is nothing to wait for.
func (c *brokerCache) Close() error {
	c.link.client.CloseIdleConnections()
	return nil
}

// readEntry reads an entry out of the owner's answer.
func readEntry(header http.Header) (Entry, error) {
	raw, err := hex.DecodeString(header.Get(outputHeader))
	var out OutputID
	if err != nil || len(raw) != len(out) {
		return Entry{}, &entryNotFoundError{Err: errBrokerMiss}
	}
	copy(out[:], raw)
	size, err := strconv.ParseInt(header.Get(sizeHeader), 10, 64)
	if err != nil {
		return Entry{}, &entryNotFoundError{Err: errBrokerMiss}
	}
	stamp := time.Time{}
	if nanos, err := strconv.ParseInt(header.Get(mtimeHeader), 10, 64); err == nil {
		stamp = time.Unix(0, nanos)
	}
	return Entry{OutputID: out, Size: size, Time: stamp}, nil
}

// readerPath names a file holding what the reader holds. An open file names
// itself; anything else is copied to a temporary file, which cleanup removes.
func readerPath(file io.ReadSeeker) (path string, cleanup func(), err error) {
	if open, ok := file.(*os.File); ok {
		if _, err := open.Seek(0, io.SeekStart); err != nil {
			return "", func() {}, err
		}
		return open.Name(), func() {}, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", func() {}, err
	}
	temp, err := os.CreateTemp("", "gocacheput-")
	if err != nil {
		return "", func() {}, err
	}
	remove := func() {
		temp.Close()
		os.Remove(temp.Name())
	}
	if _, err := io.Copy(temp, file); err != nil {
		remove()
		return "", func() {}, err
	}
	if err := temp.Close(); err != nil {
		os.Remove(temp.Name())
		return "", func() {}, err
	}
	return temp.Name(), func() { os.Remove(temp.Name()) }, nil
}

// urlValue escapes a path for the query string.
func urlValue(path string) string {
	return url.QueryEscape(path)
}
