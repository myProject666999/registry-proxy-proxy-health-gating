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
	"strings"
	"testing"
)

func TestResetAppliesHealthCheckDefaults(t *testing.T) {
	defer Reset(nil)

	err := Reset([]byte(`
enabled: true
proxies:
  docker.io: proxy-a.example.com
`))
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	hc := GetHealthCheck()
	if !hc.Enabled {
		t.Fatal("healthCheck should default to enabled")
	}
	if hc.Interval != "10s" || hc.Timeout != "2s" || hc.FreshnessTTL != "30s" {
		t.Fatalf("unexpected healthCheck defaults: %+v", hc)
	}
	if hc.FailureThreshold != 2 || hc.SuccessThreshold != 3 {
		t.Fatalf("unexpected healthCheck thresholds: %+v", hc)
	}
	if _, err := hc.Parse(); err != nil {
		t.Fatalf("default healthCheck should parse: %v", err)
	}
}

func TestResetAppliesCustomHealthCheck(t *testing.T) {
	defer Reset(nil)

	err := Reset([]byte(`
enabled: true
proxies:
  docker.io: proxy-a.example.com
healthCheck:
  enabled: true
  interval: 5s
  timeout: 1s
  failureThreshold: 3
  successThreshold: 5
  freshnessTTL: 20s
  probeImage: library/nginx:latest
`))
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	cfg, err := GetHealthCheck().Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Interval.String() != "5s" || cfg.Timeout.String() != "1s" || cfg.FreshnessTTL.String() != "20s" {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}
	if cfg.FailureThreshold != 3 || cfg.SuccessThreshold != 5 || cfg.ProbeImage != "library/nginx:latest" {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}
}

func TestResetRejectsTimeoutNotLessThanInterval(t *testing.T) {
	defer Reset(nil)

	before := GetHealthCheck()
	err := Reset([]byte(`
enabled: true
proxies:
  docker.io: proxy-a.example.com
healthCheck:
  interval: 5s
  timeout: 5s
`))
	if err == nil {
		t.Fatal("Reset should reject timeout >= interval")
	}
	if !strings.Contains(err.Error(), "must be less than interval") {
		t.Fatalf("error %q should explain that timeout must be less than interval", err.Error())
	}
	// The invalid config is rejected: the previous config stays in effect.
	if GetHealthCheck() != before {
		t.Fatal("invalid config should not replace the current config")
	}
	if GetProxies()["docker.io"] == "proxy-a.example.com" {
		t.Fatal("invalid config should not replace the current proxies")
	}
}

func TestResetRejectsStaleFreshnessTTL(t *testing.T) {
	defer Reset(nil)

	err := Reset([]byte(`
enabled: true
healthCheck:
  interval: 10s
  timeout: 2s
  freshnessTTL: 10s
`))
	if err == nil {
		t.Fatal("Reset should reject freshnessTTL <= interval")
	}
	if !strings.Contains(err.Error(), "freshnessTTL") {
		t.Fatalf("error %q should mention freshnessTTL", err.Error())
	}
}

func TestResetRejectsUnparseableYAML(t *testing.T) {
	defer Reset(nil)

	before := GetProxies()
	if err := Reset([]byte("\tnot: [valid")); err == nil {
		t.Fatal("Reset should reject unparseable YAML")
	}
	if len(GetProxies()) != len(before) {
		t.Fatal("unparseable config should not replace the current config")
	}
}
