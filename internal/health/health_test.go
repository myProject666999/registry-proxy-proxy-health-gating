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
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

var errProbe = errors.New("probe failed")

// testConfig returns a valid config with a 10s interval, 2s timeout,
// failure threshold 2, success threshold 3 and 30s freshness TTL.
func testConfig() Config {
	return Config{
		Enabled:          true,
		Interval:         10 * time.Second,
		Timeout:          2 * time.Second,
		FailureThreshold: 2,
		SuccessThreshold: 3,
		FreshnessTTL:     30 * time.Second,
		ProbeImage:       DefaultProbeImage,
	}
}

// scriptedProbe is a ProbeFunc whose per-address results are controlled by
// the test. It can block a probe on a cancellable barrier (ignoring the
// probe context, simulating a stuck proxy) and records every call.
type scriptedProbe struct {
	mu       sync.Mutex
	results  map[string]error
	barriers map[string]chan struct{}
	calls    chan string
}

func newScriptedProbe() *scriptedProbe {
	return &scriptedProbe{
		results:  map[string]error{},
		barriers: map[string]chan struct{}{},
		calls:    make(chan string, 100),
	}
}

func (p *scriptedProbe) set(addr string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results[addr] = err
}

// blockOn makes probes of addr block on the returned barrier until it is
// closed. The barrier ignores the probe context, simulating a stuck probe.
func (p *scriptedProbe) blockOn(addr string) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	b := make(chan struct{})
	p.barriers[addr] = b
	return b
}

func (p *scriptedProbe) fn() ProbeFunc {
	return func(ctx context.Context, addr string) error {
		p.calls <- addr
		p.mu.Lock()
		barrier := p.barriers[addr]
		err := p.results[addr]
		p.mu.Unlock()
		if barrier != nil {
			<-barrier
		}
		return err
	}
}

// waitCall waits for the next probe call and returns the probed address.
func (p *scriptedProbe) waitCall(t *testing.T) string {
	t.Helper()
	select {
	case addr := <-p.calls:
		return addr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for probe call")
		return ""
	}
}

func newTestChecker(t *testing.T, cfg Config, clk Clock, probe ProbeFunc) *Checker {
	t.Helper()
	c, err := NewChecker(cfg, clk, probe)
	if err != nil {
		t.Fatalf("NewChecker: %v", err)
	}
	t.Cleanup(func() {
		c.Stop()
		done := make(chan struct{})
		go func() {
			c.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("probe loops did not exit after Stop")
		}
	})
	return c
}

func clockStart() time.Time { return time.Unix(1_700_000_000, 0) }

// TestUnknownAtStartup verifies that before the first probe completes the
// instance is unknown and images are conservatively not rewritten, and that
// the first probe runs immediately (not after a full interval).
func TestUnknownAtStartup(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	barrier := probe.blockOn("proxy-a")
	c := newTestChecker(t, testConfig(), clk, probe.fn())

	c.UpdateTargets([]string{"proxy-a"})
	c.Start(context.Background())

	// The first probe is triggered immediately and is now stuck.
	if addr := probe.waitCall(t); addr != "proxy-a" {
		t.Fatalf("first probe target = %q, want proxy-a", addr)
	}

	// While the first probe has not completed, the state is unknown and the
	// admission decision is conservative: do not rewrite.
	if c.ShouldRewrite("proxy-a") {
		t.Fatal("ShouldRewrite = true before first probe completed, want false")
	}
	if got := c.Metrics().HealthyValue("proxy-a"); got != 0 {
		t.Fatalf("healthy metric = %v, want 0 (unknown)", got)
	}
	status, ok := c.Status("proxy-a")
	if !ok || status.State != StateUnknown {
		t.Fatalf("status = %+v, want state unknown", status)
	}

	// Let the first probe succeed; a single success leaves the unknown state.
	close(barrier)
	waitFor(t, "instance to become healthy", func() bool { return c.ShouldRewrite("proxy-a") })

	if got := c.Metrics().HealthyValue("proxy-a"); got != 1 {
		t.Fatalf("healthy metric = %v, want 1", got)
	}
	if got := c.Metrics().LastStateChangeValue("proxy-a"); got != float64(clk.Now().Unix()) {
		t.Fatalf("last state change metric = %v, want %v", got, clk.Now().Unix())
	}
	if got := c.Metrics().ProbesValue("proxy-a", "success"); got != 1 {
		t.Fatalf("success probes metric = %v, want 1", got)
	}
}

// TestFailureThreshold verifies hysteresis on the way down: consecutive
// failures below the threshold keep the instance healthy; reaching the
// threshold marks it unhealthy.
func TestFailureThreshold(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	c := newTestChecker(t, testConfig(), clk, probe.fn()) // failureThreshold = 2

	c.UpdateTargets([]string{"proxy-a"})
	c.Start(context.Background())
	probe.waitCall(t) // immediate first probe, success
	waitFor(t, "instance healthy", func() bool { return c.ShouldRewrite("proxy-a") })

	// First consecutive failure: still healthy, still rewriting.
	probe.set("proxy-a", errProbe)
	clk.Advance(10 * time.Second)
	probe.waitCall(t)
	waitFor(t, "failure recorded", func() bool {
		s, _ := c.Status("proxy-a")
		return s.ConsecutiveFailures == 1
	})
	if !c.ShouldRewrite("proxy-a") {
		t.Fatal("ShouldRewrite = false after 1 failure (threshold 2), want true")
	}
	if got := c.Metrics().HealthyValue("proxy-a"); got != 1 {
		t.Fatalf("healthy metric = %v after 1 failure, want 1", got)
	}

	// Second consecutive failure: threshold reached, instance unhealthy.
	clk.Advance(10 * time.Second)
	probe.waitCall(t)
	waitFor(t, "instance unhealthy", func() bool { return !c.ShouldRewrite("proxy-a") })
	status, _ := c.Status("proxy-a")
	if status.State != StateUnhealthy {
		t.Fatalf("state = %s, want unhealthy", status.State)
	}
	if got := c.Metrics().HealthyValue("proxy-a"); got != 0 {
		t.Fatalf("healthy metric = %v, want 0", got)
	}
	if got := c.Metrics().ProbesValue("proxy-a", "failure"); got != 2 {
		t.Fatalf("failure probes metric = %v, want 2", got)
	}
}

// TestRecoveryThreshold verifies hysteresis on the way up: an unhealthy
// instance recovers only after the (higher) success threshold is reached.
func TestRecoveryThreshold(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	cfg := testConfig()
	cfg.FailureThreshold = 1 // get unhealthy quickly
	cfg.SuccessThreshold = 3
	c := newTestChecker(t, cfg, clk, probe.fn())

	probe.set("proxy-a", errProbe)
	c.UpdateTargets([]string{"proxy-a"})
	c.Start(context.Background())
	probe.waitCall(t)
	waitFor(t, "instance unhealthy", func() bool {
		s, _ := c.Status("proxy-a")
		return s.State == StateUnhealthy
	})

	// Two consecutive successes: below the recovery threshold, still unhealthy.
	probe.set("proxy-a", nil)
	for i := 1; i <= 2; i++ {
		clk.Advance(10 * time.Second)
		probe.waitCall(t)
		waitFor(t, "success recorded", func() bool {
			s, _ := c.Status("proxy-a")
			return s.ConsecutiveSuccesses == i
		})
		if c.ShouldRewrite("proxy-a") {
			t.Fatalf("ShouldRewrite = true after %d successes (threshold 3), want false", i)
		}
	}

	// Third consecutive success: recovered.
	clk.Advance(10 * time.Second)
	probe.waitCall(t)
	waitFor(t, "instance recovered", func() bool { return c.ShouldRewrite("proxy-a") })
	if got := c.Metrics().HealthyValue("proxy-a"); got != 1 {
		t.Fatalf("healthy metric = %v, want 1", got)
	}
}

// TestStaleResultFallsBackToUnknown verifies that when probes get stuck and
// the latest result ages beyond the freshness TTL, the instance is treated
// as unknown again and rewriting stops.
func TestStaleResultFallsBackToUnknown(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	c := newTestChecker(t, testConfig(), clk, probe.fn()) // freshnessTTL = 30s

	c.UpdateTargets([]string{"proxy-a"})
	c.Start(context.Background())
	probe.waitCall(t)
	waitFor(t, "instance healthy", func() bool { return c.ShouldRewrite("proxy-a") })

	// The next probe gets stuck: it never returns (until the test cancels it).
	barrier := probe.blockOn("proxy-a")
	clk.Advance(10 * time.Second)
	probe.waitCall(t) // the stuck probe is now in flight

	// Within the freshness TTL the last result is still used.
	clk.Advance(20 * time.Second)
	if !c.ShouldRewrite("proxy-a") {
		t.Fatal("ShouldRewrite = false within freshness TTL, want true")
	}

	// After the TTL the result is stale: fall back to unknown, stop rewriting.
	clk.Advance(11 * time.Second) // last result is now 31s old
	if c.ShouldRewrite("proxy-a") {
		t.Fatal("ShouldRewrite = true with stale result, want false")
	}
	status, _ := c.Status("proxy-a")
	if status.State != StateHealthy {
		t.Fatalf("recorded state = %s, want healthy (staleness is applied at read time)", status.State)
	}
	if status.EffectiveState != StateUnknown {
		t.Fatalf("effective state = %s, want unknown", status.EffectiveState)
	}

	// Unblock the stuck probe; its success makes the result fresh again.
	close(barrier)
	waitFor(t, "fresh result", func() bool { return c.ShouldRewrite("proxy-a") })
}

// TestPerInstanceIndependence verifies that health is tracked per instance:
// one unhealthy instance does not affect the decision for another.
func TestPerInstanceIndependence(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	cfg := testConfig()
	cfg.FailureThreshold = 1
	c := newTestChecker(t, cfg, clk, probe.fn())

	probe.set("proxy-b", errProbe)
	c.UpdateTargets([]string{"proxy-a", "proxy-b"})
	c.Start(context.Background())

	waitFor(t, "both instances probed", func() bool {
		_, okA := c.Status("proxy-a")
		sB, okB := c.Status("proxy-b")
		return okA && okB && !sB.LastResult.IsZero() && c.ShouldRewrite("proxy-a")
	})

	if !c.ShouldRewrite("proxy-a") {
		t.Fatal("proxy-a should be rewritable")
	}
	if c.ShouldRewrite("proxy-b") {
		t.Fatal("proxy-b should not be rewritable")
	}
	if got := c.Metrics().HealthyValue("proxy-a"); got != 1 {
		t.Fatalf("proxy-a healthy metric = %v, want 1", got)
	}
	if got := c.Metrics().HealthyValue("proxy-b"); got != 0 {
		t.Fatalf("proxy-b healthy metric = %v, want 0", got)
	}
	sA, _ := c.Status("proxy-a")
	sB, _ := c.Status("proxy-b")
	if sA.State != StateHealthy || sB.State != StateUnhealthy {
		t.Fatalf("states = %s/%s, want healthy/unhealthy", sA.State, sB.State)
	}
}

// TestAdmissionDoesNotWaitForProbe verifies that the admission decision
// never blocks on a stuck probe, and that the stuck probe goroutine exits
// once its barrier is cancelled. The fake proxy blocks the probe request on
// a cancellable barrier; the probe goroutine stays stuck in the HTTP call
// until the test cancels it.
func TestAdmissionDoesNotWaitForProbe(t *testing.T) {
	fake := newFakeProxy(t)
	fake.setMode("block")
	clk := NewManualClock(clockStart())
	cfg := testConfig()
	cfg.Interval = 10 * time.Second
	cfg.Timeout = 5 * time.Second // client would eventually give up; the test cancels earlier
	c := newTestChecker(t, cfg, clk, HTTPProbe(cfg.ProbeImage, cfg.Timeout))

	c.UpdateTargets([]string{fake.url()})
	c.Start(context.Background())
	// The first probe request is now stuck on the fake proxy's barrier.
	waitFor(t, "stuck probe request", func() bool { return fake.requestCount() == 1 })

	// The admission decision must return immediately even though the probe
	// is stuck.
	done := make(chan bool, 1)
	go func() { done <- c.ShouldRewrite(fake.url()) }()
	select {
	case rewrite := <-done:
		if rewrite {
			t.Fatal("ShouldRewrite = true while instance unknown, want false")
		}
	case <-time.After(time.Second):
		t.Fatal("admission decision blocked on a stuck probe")
	}

	// Cancel the barrier: the stuck probe exits (recording its result) and
	// Wait returns, leaving no goroutine and no fake service behind.
	fake.cancelBlocked()
	waitFor(t, "probe result recorded after barrier cancellation", func() bool {
		s, ok := c.Status(fake.url())
		return ok && !s.LastResult.IsZero()
	})
	c.Stop()
	waitDone := make(chan struct{})
	go func() {
		c.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stuck probe goroutine did not exit after barrier cancellation")
	}
}

// TestStateContinuityAcrossRuleChanges verifies that hysteresis counters of
// an address survive rule reloads that keep the address, and are reset when
// the address is removed and added again.
func TestStateContinuityAcrossRuleChanges(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	c := newTestChecker(t, testConfig(), clk, probe.fn()) // failureThreshold = 2

	c.UpdateTargets([]string{"proxy-a", "proxy-b"})
	c.Start(context.Background())
	waitFor(t, "proxy-a healthy", func() bool { return c.ShouldRewrite("proxy-a") })

	// One failure: count 1, still healthy.
	probe.set("proxy-a", errProbe)
	clk.Advance(10 * time.Second)
	waitFor(t, "one failure", func() bool {
		s, _ := c.Status("proxy-a")
		return s.ConsecutiveFailures == 1
	})

	// Rule reload keeping the address: counters are retained, so the next
	// failure reaches the threshold.
	c.UpdateTargets([]string{"proxy-a"})
	clk.Advance(10 * time.Second)
	waitFor(t, "proxy-a unhealthy", func() bool {
		s, _ := c.Status("proxy-a")
		return s.State == StateUnhealthy
	})

	// Remove and re-add the address: state and counters start from scratch.
	c.UpdateTargets([]string{})
	if _, ok := c.Status("proxy-a"); ok {
		t.Fatal("removed instance should be forgotten")
	}
	probe.set("proxy-a", errProbe)
	c.UpdateTargets([]string{"proxy-a"}) // re-added: immediate first probe
	waitFor(t, "re-added instance probed", func() bool {
		s, ok := c.Status("proxy-a")
		return ok && s.ConsecutiveFailures == 1
	})
	status, _ := c.Status("proxy-a")
	if status.State != StateUnknown {
		t.Fatalf("state after re-add and 1 failure = %s, want unknown (counters reset)", status.State)
	}
	if c.ShouldRewrite("proxy-a") {
		t.Fatal("re-added instance should not be rewritable before first success")
	}

	// A single success takes the re-added instance out of unknown.
	probe.set("proxy-a", nil)
	clk.Advance(10 * time.Second)
	waitFor(t, "re-added instance healthy", func() bool { return c.ShouldRewrite("proxy-a") })
}

// TestOneProbePerInstancePerPeriod verifies that probing happens once per
// instance per period — not once per rule — and checks the probe request
// itself (path, method, headers) against a fake proxy.
func TestOneProbePerInstancePerPeriod(t *testing.T) {
	fake := newFakeProxy(t)
	clk := NewManualClock(clockStart())
	cfg := testConfig()
	cfg.Interval = time.Second
	cfg.Timeout = 500 * time.Millisecond
	cfg.FreshnessTTL = 10 * time.Second
	c := newTestChecker(t, cfg, clk, HTTPProbe(cfg.ProbeImage, cfg.Timeout))

	// Two rules point at the same instance; the address must be probed once.
	c.UpdateTargets([]string{fake.url(), fake.url()})
	c.Start(context.Background())

	// The first probe happens immediately at startup.
	waitFor(t, "first probe request", func() bool { return fake.requestCount() == 1 })

	req := fake.request(0)
	if req.Method != "HEAD" {
		t.Fatalf("probe method = %s, want HEAD", req.Method)
	}
	if req.Path != "/v2/library/alpine/manifests/latest" {
		t.Fatalf("probe path = %s, want /v2/library/alpine/manifests/latest", req.Path)
	}
	if accept := req.Header.Get("Accept"); !strings.Contains(accept, "application/vnd.docker.distribution.manifest.v2+json") {
		t.Fatalf("probe Accept header = %q, want manifest media types", accept)
	}
	if ua := req.Header.Get("User-Agent"); ua != userAgent {
		t.Fatalf("probe User-Agent = %q, want %q", ua, userAgent)
	}

	// Each further period produces exactly one more request.
	clk.Advance(time.Second)
	waitFor(t, "second probe request", func() bool { return fake.requestCount() == 2 })
	clk.Advance(time.Second)
	waitFor(t, "third probe request", func() bool { return fake.requestCount() == 3 })

	if got := c.Metrics().ProbesValue(fake.url(), "success"); got != 3 {
		t.Fatalf("success probes metric = %v, want 3", got)
	}
	if !c.ShouldRewrite(fake.url()) {
		t.Fatal("fake proxy is ok, should be rewritable")
	}
}

// TestProbeTimeout verifies that a proxy that accepts connections but never
// responds is failed by the probe timeout (a manifest-level check, not a
// port check).
func TestProbeTimeout(t *testing.T) {
	fake := newFakeProxy(t)
	fake.setMode("timeout")
	clk := NewManualClock(clockStart())
	cfg := testConfig()
	cfg.Interval = time.Second
	cfg.Timeout = 100 * time.Millisecond
	cfg.FreshnessTTL = 10 * time.Second
	cfg.FailureThreshold = 1
	c := newTestChecker(t, cfg, clk, HTTPProbe(cfg.ProbeImage, cfg.Timeout))

	c.UpdateTargets([]string{fake.url()})
	c.Start(context.Background())

	waitFor(t, "instance unhealthy after probe timeout", func() bool {
		s, ok := c.Status(fake.url())
		return ok && s.State == StateUnhealthy
	})
	if c.ShouldRewrite(fake.url()) {
		t.Fatal("unresponsive proxy should not be rewritable")
	}
	if got := c.Metrics().ProbesValue(fake.url(), "failure"); got < 1 {
		t.Fatalf("failure probes metric = %v, want >= 1", got)
	}
}

// TestProbeErrorResponse verifies that a proxy answering with an error
// status is considered unhealthy.
func TestProbeErrorResponse(t *testing.T) {
	fake := newFakeProxy(t)
	fake.setMode("error")
	clk := NewManualClock(clockStart())
	cfg := testConfig()
	cfg.Interval = time.Second
	cfg.Timeout = 500 * time.Millisecond
	cfg.FreshnessTTL = 10 * time.Second
	cfg.FailureThreshold = 1
	c := newTestChecker(t, cfg, clk, HTTPProbe(cfg.ProbeImage, cfg.Timeout))

	c.UpdateTargets([]string{fake.url()})
	c.Start(context.Background())

	waitFor(t, "instance unhealthy after error response", func() bool {
		s, ok := c.Status(fake.url())
		return ok && s.State == StateUnhealthy
	})
	if c.ShouldRewrite(fake.url()) {
		t.Fatal("erroring proxy should not be rewritable")
	}
}

// TestDisabledCheckerAlwaysRewrites verifies that with health checking
// disabled the decision is always to rewrite and no probing happens.
func TestDisabledCheckerAlwaysRewrites(t *testing.T) {
	probe := newScriptedProbe()
	c := newTestChecker(t, Config{Enabled: false}, NewManualClock(clockStart()), probe.fn())
	c.UpdateTargets([]string{"proxy-a"})
	c.Start(context.Background())

	if !c.ShouldRewrite("proxy-a") {
		t.Fatal("disabled checker should always allow rewriting")
	}
	if !c.ShouldRewrite("never-heard-of-it") {
		t.Fatal("disabled checker should always allow rewriting")
	}
	select {
	case addr := <-probe.calls:
		t.Fatalf("disabled checker probed %s, want no probes", addr)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestConfigValidation verifies that invalid configurations are rejected
// with clear messages, in particular timeout >= interval.
func TestConfigValidation(t *testing.T) {
	valid := testConfig()
	if err := valid.Validate(); err != nil {
		t.Fatalf("default test config should be valid: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"timeout equals interval", func(c *Config) { c.Timeout = c.Interval }, "must be less than interval"},
		{"timeout exceeds interval", func(c *Config) { c.Timeout = 2 * c.Interval }, "must be less than interval"},
		{"zero interval", func(c *Config) { c.Interval = 0 }, "interval"},
		{"zero timeout", func(c *Config) { c.Timeout = 0 }, "timeout"},
		{"zero failure threshold", func(c *Config) { c.FailureThreshold = 0 }, "failureThreshold"},
		{"zero success threshold", func(c *Config) { c.SuccessThreshold = 0 }, "successThreshold"},
		{"freshness TTL below interval", func(c *Config) { c.FreshnessTTL = c.Interval }, "freshnessTTL"},
		{"empty probe image", func(c *Config) { c.ProbeImage = "" }, "probeImage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
			if _, err := NewChecker(cfg, nil, nil); err == nil {
				t.Fatal("NewChecker should reject invalid config")
			}
		})
	}

	// A disabled config skips validation.
	disabled := Config{Enabled: false}
	if err := disabled.Validate(); err != nil {
		t.Fatalf("disabled config should be valid: %v", err)
	}
}

// TestNewTargetProbedImmediately verifies that a target added after Start is
// probed immediately, so the unknown window is one probe round-trip rather
// than a full interval.
func TestNewTargetProbedImmediately(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	c := newTestChecker(t, testConfig(), clk, probe.fn())
	c.Start(context.Background())

	// Added after start: probed immediately, without advancing the clock.
	c.UpdateTargets([]string{"proxy-late"})
	if addr := probe.waitCall(t); addr != "proxy-late" {
		t.Fatalf("probe target = %q, want proxy-late", addr)
	}
	waitFor(t, "late target healthy", func() bool { return c.ShouldRewrite("proxy-late") })
}

// TestGoroutineCleanup verifies that Stop terminates all probe loops of the
// module, tracked by its own WaitGroup.
func TestGoroutineCleanup(t *testing.T) {
	clk := NewManualClock(clockStart())
	probe := newScriptedProbe()
	c, err := NewChecker(testConfig(), clk, probe.fn())
	if err != nil {
		t.Fatal(err)
	}
	c.UpdateTargets([]string{"proxy-a", "proxy-b", "proxy-c"})
	c.Start(context.Background())
	waitFor(t, "all instances healthy", func() bool {
		return c.ShouldRewrite("proxy-a") && c.ShouldRewrite("proxy-b") && c.ShouldRewrite("proxy-c")
	})

	c.Stop()
	done := make(chan struct{})
	go func() {
		c.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe loops did not exit after Stop")
	}
}
