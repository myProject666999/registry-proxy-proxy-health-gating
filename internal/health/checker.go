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
	"log"
	"sync"
	"time"
)

// State is the health state of a proxy instance.
type State string

const (
	// StateUnknown means no fresh probe result is available yet (startup,
	// newly added target, or a stale result). The conservative decision is
	// to not rewrite images.
	StateUnknown State = "unknown"
	// StateHealthy means the instance is considered healthy; images may be
	// rewritten to it.
	StateHealthy State = "healthy"
	// StateUnhealthy means the instance is considered unhealthy; images must
	// keep their original reference.
	StateUnhealthy State = "unhealthy"
)

// ProbeFunc probes one proxy instance and returns nil when the instance is
// healthy. It must respect ctx cancellation; the ctx carries the configured
// probe timeout.
type ProbeFunc func(ctx context.Context, addr string) error

// InstanceStatus is a snapshot of one instance's state, for tests and
// introspection.
type InstanceStatus struct {
	State                State
	EffectiveState       State
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	LastResult           time.Time
	LastStateChange      time.Time
}

type instance struct {
	addr string

	state          State
	consecFailures int
	consecSuccess  int
	lastResult     time.Time
	lastChange     time.Time

	// cancel stops this instance's probe loop (target removed or checker
	// stopped).
	cancel context.CancelFunc
}

// Checker tracks the health of proxy instances. Probing happens in
// background goroutines; the admission path only reads the latest results
// via ShouldRewrite and never blocks on probing.
type Checker struct {
	cfg    Config
	clock  Clock
	probe  ProbeFunc
	metric *Metrics

	mu        sync.RWMutex
	instances map[string]*instance

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// NewChecker creates a Checker. probe may be nil when cfg.Enabled is false.
func NewChecker(cfg Config, clock Clock, probe ProbeFunc) (*Checker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = RealClock{}
	}
	if probe == nil {
		probe = func(context.Context, string) error { return nil }
	}
	return &Checker{
		cfg:       cfg,
		clock:     clock,
		probe:     probe,
		metric:    newMetrics(),
		instances: map[string]*instance{},
	}, nil
}

// Config returns the checker's configuration.
func (c *Checker) Config() Config { return c.cfg }

// Metrics returns the checker's metrics.
func (c *Checker) Metrics() *Metrics { return c.metric }

// Start launches the background probe loops for the current targets. It is
// a no-op when the checker is disabled or already started.
func (c *Checker) Start(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || !c.cfg.Enabled {
		return
	}
	c.started = true
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, inst := range c.instances {
		c.startInstanceLocked(inst)
	}
}

// Stop cancels all probe loops. Use Wait to block until they have exited.
func (c *Checker) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
	for _, inst := range c.instances {
		if inst.cancel != nil {
			inst.cancel()
		}
	}
}

// Wait blocks until all probe loops started by this checker have exited.
func (c *Checker) Wait() { c.wg.Wait() }

// UpdateTargets reconciles the set of probed instances with the currently
// effective proxy rules. State and hysteresis counters of instances that
// remain in the set are retained; removed instances are forgotten (so an
// address that is removed and later added again starts from scratch); newly
// added instances start in StateUnknown and are probed immediately.
func (c *Checker) UpdateTargets(addrs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	want := map[string]struct{}{}
	for _, a := range addrs {
		if a == "" {
			continue
		}
		want[a] = struct{}{}
	}

	// Remove instances no longer referenced by any rule.
	for addr, inst := range c.instances {
		if _, ok := want[addr]; ok {
			continue
		}
		if inst.cancel != nil {
			inst.cancel()
		}
		delete(c.instances, addr)
		c.metric.remove(addr)
		log.Printf("Proxy instance %s removed from health check targets", addr)
	}

	// Add new instances in unknown state.
	for addr := range want {
		if _, ok := c.instances[addr]; ok {
			continue
		}
		inst := &instance{addr: addr, state: StateUnknown}
		c.instances[addr] = inst
		c.metric.setState(addr, StateUnknown, c.clock.Now())
		log.Printf("Proxy instance %s added to health check targets (state: %s)", addr, StateUnknown)
		if c.started {
			c.startInstanceLocked(inst)
		}
	}
}

// startInstanceLocked starts the probe loop of one instance. The first probe
// runs immediately so that a new target leaves the unknown state as fast as
// possible instead of waiting a full interval. Callers must hold c.mu.
func (c *Checker) startInstanceLocked(inst *instance) {
	ctx, cancel := context.WithCancel(c.ctx)
	inst.cancel = cancel
	c.wg.Add(1)
	go c.probeLoop(ctx, inst.addr)
}

func (c *Checker) probeLoop(ctx context.Context, addr string) {
	defer c.wg.Done()

	c.probeOnce(ctx, addr)

	ticker := c.clock.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			c.probeOnce(ctx, addr)
		}
	}
}

func (c *Checker) probeOnce(ctx context.Context, addr string) {
	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	err := c.probe(probeCtx, addr)
	cancel()
	c.recordResult(addr, err == nil)
}

func (c *Checker) recordResult(addr string, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, ok := c.instances[addr]
	if !ok {
		// Target was removed while the probe was in flight.
		return
	}

	now := c.clock.Now()
	inst.lastResult = now
	c.metric.incProbe(addr, success)
	c.metric.setLastProbe(addr, now)

	if success {
		inst.consecSuccess++
		inst.consecFailures = 0
	} else {
		inst.consecFailures++
		inst.consecSuccess = 0
	}

	old := inst.state
	switch inst.state {
	case StateUnknown:
		// From unknown a single success is enough: there is no evidence of
		// trouble, and this minimizes the window without acceleration after
		// startup or after a rule is added. Failures are counted towards the
		// same failure threshold as for healthy instances.
		if success && inst.consecSuccess >= 1 {
			inst.state = StateHealthy
		} else if !success && inst.consecFailures >= c.cfg.FailureThreshold {
			inst.state = StateUnhealthy
		}
	case StateHealthy:
		if !success && inst.consecFailures >= c.cfg.FailureThreshold {
			inst.state = StateUnhealthy
		}
	case StateUnhealthy:
		if success && inst.consecSuccess >= c.cfg.SuccessThreshold {
			inst.state = StateHealthy
		}
	}

	if inst.state != old {
		inst.lastChange = now
		c.metric.setState(addr, inst.state, now)
		log.Printf("Proxy instance %s health state changed: %s -> %s", addr, old, inst.state)
	}
}

// ShouldRewrite reports whether images may be rewritten to the proxy
// instance addr. It never blocks on probing: it only reads the latest
// background result. It returns false (the conservative decision) when the
// instance is unknown, unhealthy, or its latest result is stale. When health
// checking is disabled it always returns true.
func (c *Checker) ShouldRewrite(addr string) bool {
	if !c.cfg.Enabled {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.effectiveStateLocked(addr, c.clock.Now()) == StateHealthy
}

// Status returns a snapshot of one instance's state.
func (c *Checker) Status(addr string) (InstanceStatus, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	inst, ok := c.instances[addr]
	if !ok {
		return InstanceStatus{}, false
	}
	now := c.clock.Now()
	return InstanceStatus{
		State:                inst.state,
		EffectiveState:       c.effectiveStateLocked(addr, now),
		ConsecutiveFailures:  inst.consecFailures,
		ConsecutiveSuccesses: inst.consecSuccess,
		LastResult:           inst.lastResult,
		LastStateChange:      inst.lastChange,
	}, true
}

// effectiveStateLocked degrades the recorded state to unknown when the
// latest result is stale (e.g. a probe is stuck). Callers must hold c.mu.
func (c *Checker) effectiveStateLocked(addr string, now time.Time) State {
	inst, ok := c.instances[addr]
	if !ok {
		return StateUnknown
	}
	if inst.lastResult.IsZero() || now.Sub(inst.lastResult) > c.cfg.FreshnessTTL {
		return StateUnknown
	}
	return inst.state
}
