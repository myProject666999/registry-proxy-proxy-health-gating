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
	"fmt"
	"time"
)

const (
	// DefaultInterval is the default period between two probes of the same instance.
	DefaultInterval = 10 * time.Second
	// DefaultTimeout is the default timeout of a single probe request.
	DefaultTimeout = 2 * time.Second
	// DefaultFailureThreshold is the default number of consecutive failures
	// required to mark an instance unhealthy.
	DefaultFailureThreshold = 2
	// DefaultSuccessThreshold is the default number of consecutive successes
	// required to mark an unhealthy instance recovered.
	DefaultSuccessThreshold = 3
	// DefaultFreshnessTTL is the default max age of the latest probe result
	// before it is considered stale and the instance falls back to unknown.
	DefaultFreshnessTTL = 30 * time.Second
	// DefaultProbeImage is the default image (repository:tag, without registry
	// host) whose manifest is requested to probe a proxy instance.
	DefaultProbeImage = "library/alpine:latest"
)

// Config configures the health Checker.
type Config struct {
	// Enabled toggles health gating. When false, ShouldRewrite always returns
	// true and no probing happens.
	Enabled bool
	// Interval is the period between two probes of the same instance.
	Interval time.Duration
	// Timeout is the timeout of a single probe request. It must be less than
	// Interval so that probes cannot overlap and pile up.
	Timeout time.Duration
	// FailureThreshold is the number of consecutive probe failures required
	// to mark an instance unhealthy.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive probe successes required
	// to mark an unhealthy instance healthy again.
	SuccessThreshold int
	// FreshnessTTL is the max age of the latest probe result. If no probe
	// completed within this window (e.g. a probe is stuck), the instance is
	// treated as unknown. It must be greater than Interval.
	FreshnessTTL time.Duration
	// ProbeImage is the image (repository:tag, without registry host) whose
	// manifest is requested on every probe.
	ProbeImage string
}

// DefaultConfig returns the default health check configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:          true,
		Interval:         DefaultInterval,
		Timeout:          DefaultTimeout,
		FailureThreshold: DefaultFailureThreshold,
		SuccessThreshold: DefaultSuccessThreshold,
		FreshnessTTL:     DefaultFreshnessTTL,
		ProbeImage:       DefaultProbeImage,
	}
}

// Validate validates the configuration. In particular it rejects combinations
// where Timeout >= Interval (probes could overlap and pile up) and where
// FreshnessTTL <= Interval (a healthy instance would look permanently stale).
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Interval <= 0 {
		return fmt.Errorf("health check interval (%s) must be positive", c.Interval)
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("health check timeout (%s) must be positive", c.Timeout)
	}
	if c.Timeout >= c.Interval {
		return fmt.Errorf("health check timeout (%s) must be less than interval (%s), otherwise probes may overlap and pile up", c.Timeout, c.Interval)
	}
	if c.FailureThreshold < 1 {
		return fmt.Errorf("health check failureThreshold (%d) must be at least 1", c.FailureThreshold)
	}
	if c.SuccessThreshold < 1 {
		return fmt.Errorf("health check successThreshold (%d) must be at least 1", c.SuccessThreshold)
	}
	if c.FreshnessTTL <= c.Interval {
		return fmt.Errorf("health check freshnessTTL (%s) must be greater than interval (%s), otherwise even healthy instances would be treated as stale", c.FreshnessTTL, c.Interval)
	}
	if c.ProbeImage == "" {
		return fmt.Errorf("health check probeImage must not be empty")
	}
	return nil
}
