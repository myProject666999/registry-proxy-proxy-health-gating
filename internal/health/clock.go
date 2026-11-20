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
	"sync"
	"time"
)

// Clock abstracts time so that tests can inject a manually controlled clock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTicker returns a Ticker firing every d.
	NewTicker(d time.Duration) Ticker
}

// Ticker abstracts time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock is the production Clock backed by the time package.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// NewTicker implements Clock.
func (RealClock) NewTicker(d time.Duration) Ticker { return realTicker{t: time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// ManualClock is a Clock whose time only advances when Advance is called.
// Tickers created from it fire when Advance accumulates at least their
// period. It is intended for tests.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers map[*manualTicker]struct{}
}

// NewManualClock creates a ManualClock starting at start.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start, tickers: map[*manualTicker]struct{}{}}
}

// Now implements Clock.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTicker implements Clock.
func (c *ManualClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTicker{period: d, ch: make(chan time.Time, 1), clock: c}
	c.tickers[t] = struct{}{}
	return t
}

// Advance moves the clock forward by d and fires any tickers whose period
// has elapsed. Like time.Ticker, at most one tick is delivered per period
// boundary; a slow receiver drops ticks.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for t := range c.tickers {
		t.acc += d
		if t.acc >= t.period {
			t.acc = t.acc % t.period
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}

type manualTicker struct {
	period time.Duration
	acc    time.Duration
	ch     chan time.Time
	clock  *ManualClock
}

func (t *manualTicker) C() <-chan time.Time { return t.ch }

func (t *manualTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	delete(t.clock.tickers, t)
}
