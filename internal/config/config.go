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

package config

import (
	"log"
	"time"

	"github.com/ketches/registry-proxy/internal/health"
	"github.com/ketches/registry-proxy/pkg/util"
	"k8s.io/apimachinery/pkg/labels"
)

// config is the registry proxy config
type config struct {
	// Enabled is the flag to enable the registry proxy
	Enabled bool `yaml:"enabled"`
	// Proxies is the map of registry domain and proxy domain
	Proxies map[string]string `yaml:"proxies"`
	// ExcludeNamespaces is the list of namespaces that will not be proxied
	ExcludeNamespaces []string `yaml:"excludeNamespaces"`
	// IncludeNamespaces is the list of namespaces that will be proxied
	IncludeNamespaces []string `yaml:"includeNamespaces"`
	// PodSelector is the selector to select the pods that will be proxied
	PodSelector labels.Set `yaml:"podSelector"`
	// NamespaceSelector is the selector to select the namespaces that will be proxied
	NamespaceSelector labels.Set `yaml:"namespaceSelector"`
	// HealthCheck configures the background health checking of proxy instances
	HealthCheck HealthCheckConfig `yaml:"healthCheck"`
}

// HealthCheckConfig is the YAML representation of the health check
// configuration. Durations are strings such as "10s".
type HealthCheckConfig struct {
	// Enabled toggles health gating of image rewriting
	Enabled bool `yaml:"enabled"`
	// Interval is the period between two probes of the same proxy instance
	Interval string `yaml:"interval"`
	// Timeout is the timeout of a single probe request, must be less than Interval
	Timeout string `yaml:"timeout"`
	// FailureThreshold is the number of consecutive failures before an
	// instance is marked unhealthy
	FailureThreshold int `yaml:"failureThreshold"`
	// SuccessThreshold is the number of consecutive successes before an
	// unhealthy instance is marked healthy again
	SuccessThreshold int `yaml:"successThreshold"`
	// FreshnessTTL is the max age of the latest probe result before the
	// instance is treated as unknown, must be greater than Interval
	FreshnessTTL string `yaml:"freshnessTTL"`
	// ProbeImage is the image (repository:tag, without registry host) whose
	// manifest is requested on every probe
	ProbeImage string `yaml:"probeImage"`
}

// defaultHealthCheckConfig returns the default health check configuration.
func defaultHealthCheckConfig() HealthCheckConfig {
	d := health.DefaultConfig()
	return HealthCheckConfig{
		Enabled:          d.Enabled,
		Interval:         d.Interval.String(),
		Timeout:          d.Timeout.String(),
		FailureThreshold: d.FailureThreshold,
		SuccessThreshold: d.SuccessThreshold,
		FreshnessTTL:     d.FreshnessTTL.String(),
		ProbeImage:       d.ProbeImage,
	}
}

// UnmarshalYAML pre-fills defaults so that omitted fields keep their
// default values.
func (h *HealthCheckConfig) UnmarshalYAML(unmarshal func(any) error) error {
	*h = defaultHealthCheckConfig()
	type plain HealthCheckConfig
	return unmarshal((*plain)(h))
}

// isZero reports whether the section was absent from the YAML document.
func (h HealthCheckConfig) isZero() bool {
	return h == HealthCheckConfig{}
}

// Parse parses and validates the health check configuration.
func (h HealthCheckConfig) Parse() (health.Config, error) {
	cfg := health.Config{
		Enabled:          h.Enabled,
		FailureThreshold: h.FailureThreshold,
		SuccessThreshold: h.SuccessThreshold,
		ProbeImage:       h.ProbeImage,
	}
	var err error
	if cfg.Interval, err = time.ParseDuration(h.Interval); err != nil {
		return cfg, errInvalidHealthCheck("interval", h.Interval, err)
	}
	if cfg.Timeout, err = time.ParseDuration(h.Timeout); err != nil {
		return cfg, errInvalidHealthCheck("timeout", h.Timeout, err)
	}
	if cfg.FreshnessTTL, err = time.ParseDuration(h.FreshnessTTL); err != nil {
		return cfg, errInvalidHealthCheck("freshnessTTL", h.FreshnessTTL, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func errInvalidHealthCheck(field, value string, err error) error {
	return &HealthCheckError{Field: field, Value: value, Err: err}
}

// HealthCheckError describes an invalid health check configuration field.
type HealthCheckError struct {
	Field string
	Value string
	Err   error
}

func (e *HealthCheckError) Error() string {
	return "invalid healthCheck." + e.Field + " (" + e.Value + "): " + e.Err.Error()
}

var defaultProxies = map[string]string{
	"docker.io":       "docker.linkos.org",
	"registry.k8s.io": "k8s.linkos.org",
	"quay.io":         "quay.linkos.org",
	"ghcr.io":         "ghcr.linkos.org",
	"gcr.io":          "gcr.linkos.org",
}

var defaultConfig = config{
	Enabled: true,
	Proxies: defaultProxies,
	ExcludeNamespaces: []string{
		"kube-system",
		"kube-public",
		"kube-node-lease",
		"registry-proxy",
	},
	IncludeNamespaces: []string{
		"*",
	},
	PodSelector:       labels.Set{},
	NamespaceSelector: labels.Set{},
	HealthCheck:       defaultHealthCheckConfig(),
}

// configInstance is the singleton config instance
var configInstance = &defaultConfig

// onResetCallbacks are called after a successful Reset.
var onResetCallbacks []func()

// Get get the singleton config instance
func Get() *config {
	return configInstance
}

// GetProxies get the singleton config instance's proxies
func GetProxies() map[string]string {
	return configInstance.Proxies
}

// GetExcludeNamespaces get the singleton config instance's excludeNamespaces
func GetExcludeNamespaces() []string {
	return configInstance.ExcludeNamespaces
}

// GetIncludeNamespaces get the singleton config instance's includeNamespaces
func GetIncludeNamespaces() []string {
	return configInstance.IncludeNamespaces
}

// Enabled get the singleton config instance's enabled
func Enabled() bool {
	return configInstance.Enabled
}

// PodSelector get the singleton config instance's pod selector
func PodSelector() labels.Set {
	return configInstance.PodSelector
}

// NamespaceSelector get the singleton config instance's namespace selector
func NamespaceSelector() labels.Set {
	return configInstance.NamespaceSelector
}

// GetHealthCheck get the singleton config instance's health check config
func GetHealthCheck() HealthCheckConfig {
	return configInstance.HealthCheck
}

// RegisterOnReset registers a callback invoked after every successful Reset.
func RegisterOnReset(fn func()) {
	onResetCallbacks = append(onResetCallbacks, fn)
}

// Reset resets the singleton config instance.
// If in is empty, reset to default config.
// If in holds an invalid config (unparseable, or an invalid healthCheck
// section such as timeout >= interval), Reset rejects it, keeps the current
// config, and returns an error describing the problem.
func Reset(in []byte) error {
	if len(in) > 0 {
		candidate := &config{}
		if err := util.UnmarshalYAML(in, candidate); err != nil {
			log.Printf("Rejecting config: unmarshal failed: %v", err)
			return err
		}
		if candidate.HealthCheck.isZero() {
			candidate.HealthCheck = defaultHealthCheckConfig()
		}
		if _, err := candidate.HealthCheck.Parse(); err != nil {
			log.Printf("Rejecting config: %v", err)
			return err
		}
		configInstance = candidate
	} else {
		// reset to default config
		configInstance = &defaultConfig
	}
	printCurrentConfig()
	for _, fn := range onResetCallbacks {
		fn()
	}
	return nil
}

// printCurrentConfig print current config
func printCurrentConfig() {
	out, err := util.MarshalYAML(configInstance)
	if err != nil {
		log.Printf("Marshal config failed: %v", err)
		return
	}
	log.Printf("Current registry proxy config: \n%s", string(out))
}
