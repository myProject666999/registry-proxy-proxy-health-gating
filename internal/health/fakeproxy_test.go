/*
Copyright 2024 The Ketches Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package health

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// recordedRequest captures what the fake proxy received.
type recordedRequest struct {
	Method string
	Path   string
	Header http.Header
}

// fakeProxy is an httptest-based fake registry proxy whose behavior can be
// switched at any time: "ok" returns 200, "error" returns 500, "timeout"
// sleeps beyond any reasonable client timeout, "block" waits on a
// cancellable barrier. It records every request it receives.
type fakeProxy struct {
	srv *httptest.Server

	mu       sync.Mutex
	mode     string
	barrier  chan struct{}
	requests []recordedRequest
}

func newFakeProxy(t *testing.T) *fakeProxy {
	t.Helper()
	f := &fakeProxy{mode: "ok", barrier: make(chan struct{})}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone()})
		mode := f.mode
		barrier := f.barrier
		f.mu.Unlock()

		switch mode {
		case "ok":
			w.WriteHeader(http.StatusOK)
		case "error":
			w.WriteHeader(http.StatusInternalServerError)
		case "timeout":
			// Longer than any client timeout used in tests; the request
			// context is done when the client gives up.
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
		case "block":
			// Block on a barrier the test can cancel; also respect client
			// cancellation so the fake server can always be reclaimed.
			select {
			case <-barrier:
			case <-r.Context().Done():
			}
		}
	}))
	t.Cleanup(f.close)
	return f
}

// setMode switches the behavior of the fake proxy.
func (f *fakeProxy) setMode(mode string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = mode
}

// cancelBlocked releases requests blocked in "block" mode.
func (f *fakeProxy) cancelBlocked() {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.barrier:
	default:
		close(f.barrier)
	}
}

// url returns the base URL of the fake proxy, usable as a probe target.
func (f *fakeProxy) url() string { return f.srv.URL }

// requestCount returns the number of requests received so far.
func (f *fakeProxy) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// request returns the i-th recorded request.
func (f *fakeProxy) request(i int) recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[i]
}

// close releases blocked handlers and shuts the server down, so no fake
// service is left behind.
func (f *fakeProxy) close() {
	f.cancelBlocked()
	f.srv.Close()
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
