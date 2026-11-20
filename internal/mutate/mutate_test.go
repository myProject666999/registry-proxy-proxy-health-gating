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

package mutate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ketches/registry-proxy/internal/config"
	"github.com/ketches/registry-proxy/internal/health"
	corev1 "k8s.io/api/core/v1"
)

const testConfigYAML = `
enabled: true
proxies:
  docker.io: proxy-a.example.com
  quay.io: proxy-b.example.com
  ghcr.io: proxy-a.example.com
includeNamespaces:
- "*"
`

// probeFunc is a test probe whose per-address results are set up front.
type probeFunc struct {
	mu      sync.Mutex
	results map[string]error
}

func (p *probeFunc) set(addr string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results[addr] = err
}

func (p *probeFunc) fn() health.ProbeFunc {
	return func(ctx context.Context, addr string) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.results[addr]
	}
}

// setupHealth installs a test checker as the health default and restores
// everything on cleanup.
func setupHealth(t *testing.T, cfg health.Config, probe health.ProbeFunc) (*health.Checker, *health.ManualClock) {
	t.Helper()
	if err := config.Reset([]byte(testConfigYAML)); err != nil {
		t.Fatalf("config.Reset: %v", err)
	}
	clk := health.NewManualClock(time.Unix(1_700_000_000, 0))
	checker, err := health.NewChecker(cfg, clk, probe)
	if err != nil {
		t.Fatalf("health.NewChecker: %v", err)
	}
	health.SetDefault(checker)
	t.Cleanup(func() {
		checker.Stop()
		checker.Wait()
		health.SetDefault(nil)
		config.Reset(nil)
	})
	return checker, clk
}

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

func testPod() *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "init", Image: "busybox:1.36"},
			},
			Containers: []corev1.Container{
				{Name: "web", Image: "nginx:1.25"},
				{Name: "app", Image: "quay.io/org/app:2"},
				{Name: "tool", Image: "ghcr.io/org/tool:3"},
			},
		},
	}
}

// TestPodRewriteGatedByInstanceHealth verifies that with two proxy instances
// in different health states, each image is judged independently against the
// instance its rule points to.
func TestPodRewriteGatedByInstanceHealth(t *testing.T) {
	cfg := health.DefaultConfig()
	cfg.FailureThreshold = 1
	probe := &probeFunc{results: map[string]error{
		"proxy-b.example.com": errors.New("proxy-b is down"),
	}}
	checker, _ := setupHealth(t, cfg, probe.fn())

	checker.UpdateTargets([]string{"proxy-a.example.com", "proxy-b.example.com"})
	checker.Start(context.Background())
	waitFor(t, "proxy-a healthy and proxy-b unhealthy", func() bool {
		return checker.ShouldRewrite("proxy-a.example.com") && !checker.ShouldRewrite("proxy-b.example.com")
	})

	pod := testPod()
	Pod(pod)

	want := []string{
		"proxy-a.example.com/library/busybox:1.36", // init container, healthy instance
		"proxy-a.example.com/library/nginx:1.25",   // healthy instance
		"quay.io/org/app:2",                        // unhealthy instance: keep original
		"proxy-a.example.com/org/tool:3",           // healthy instance
	}
	got := []string{
		pod.Spec.InitContainers[0].Image,
		pod.Spec.Containers[0].Image,
		pod.Spec.Containers[1].Image,
		pod.Spec.Containers[2].Image,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("image %d = %q, want %q", i, got[i], want[i])
		}
	}

	// The exposed metrics reflect the per-instance states.
	if v := checker.Metrics().HealthyValue("proxy-a.example.com"); v != 1 {
		t.Errorf("proxy-a healthy metric = %v, want 1", v)
	}
	if v := checker.Metrics().HealthyValue("proxy-b.example.com"); v != 0 {
		t.Errorf("proxy-b healthy metric = %v, want 0", v)
	}
}

// TestPodRewriteUnknownInstance verifies that before the first probe of an
// instance completes, images pointing to it keep their original reference.
func TestPodRewriteUnknownInstance(t *testing.T) {
	checker, _ := setupHealth(t, health.DefaultConfig(), (&probeFunc{results: map[string]error{}}).fn())
	checker.UpdateTargets([]string{"proxy-a.example.com", "proxy-b.example.com"})
	// Not started: no probe has completed, everything is unknown.

	pod := testPod()
	Pod(pod)

	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if strings.Contains(c.Image, "proxy-") {
			t.Errorf("image %q was rewritten while all instances unknown", c.Image)
		}
	}
}

// TestPodRewriteHealthCheckDisabled verifies that with health checking
// disabled every matched image is rewritten (the pre-gating behavior).
func TestPodRewriteHealthCheckDisabled(t *testing.T) {
	setupHealth(t, health.Config{Enabled: false}, nil)

	pod := testPod()
	Pod(pod)

	want := []string{
		"proxy-a.example.com/library/busybox:1.36",
		"proxy-a.example.com/library/nginx:1.25",
		"proxy-b.example.com/org/app:2",
		"proxy-a.example.com/org/tool:3",
	}
	got := []string{
		pod.Spec.InitContainers[0].Image,
		pod.Spec.Containers[0].Image,
		pod.Spec.Containers[1].Image,
		pod.Spec.Containers[2].Image,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("image %d = %q, want %q", i, got[i], want[i])
		}
	}
}
