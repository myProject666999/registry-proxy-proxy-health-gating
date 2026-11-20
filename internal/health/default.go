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
	"sync"
)

var (
	defaultMu      sync.Mutex
	defaultChecker *Checker
)

// Default returns the process-wide checker. Before ReconfigureDefault is
// called it is a disabled checker (ShouldRewrite always returns true), which
// preserves the pre-health-gating behavior.
func Default() *Checker {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultChecker == nil {
		defaultChecker, _ = NewChecker(Config{Enabled: false}, RealClock{}, nil)
	}
	return defaultChecker
}

// SetDefault replaces the process-wide checker. It is intended for tests.
func SetDefault(c *Checker) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	defaultChecker = c
}

// ReconfigureDefault applies cfg and targets to the process-wide checker.
//
// If cfg differs from the current configuration (including the first call),
// the checker is rebuilt: the old one is stopped and a new one with a real
// clock and the HTTP manifest probe is started. If only the targets changed
// (the common case of a rule hot-reload), the existing checker is kept and
// UpdateTargets reconciles the probe set, retaining per-instance state and
// hysteresis counters for addresses that remain.
func ReconfigureDefault(cfg Config, targets []string) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	defaultMu.Lock()
	defer defaultMu.Unlock()

	if defaultChecker != nil && defaultChecker.cfg == cfg {
		defaultChecker.UpdateTargets(targets)
		return nil
	}

	if defaultChecker != nil {
		defaultChecker.Stop()
		defaultChecker.Wait()
	}

	checker, err := NewChecker(cfg, RealClock{}, HTTPProbe(cfg.ProbeImage, cfg.Timeout))
	if err != nil {
		return err
	}
	checker.UpdateTargets(targets)
	checker.Start(context.Background())
	defaultChecker = checker
	return nil
}

// ShutdownDefault stops the process-wide checker and waits for its probe
// loops to exit.
func ShutdownDefault() {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultChecker != nil {
		defaultChecker.Stop()
		defaultChecker.Wait()
	}
}
