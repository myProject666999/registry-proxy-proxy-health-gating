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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// Metrics holds the Prometheus collectors exposing per-instance health
// state. Each Checker owns its own registry so tests can run many checkers
// without interference.
type Metrics struct {
	registry *prometheus.Registry

	// healthy is 1 when the instance's recorded state is healthy, 0
	// otherwise (unknown or unhealthy).
	healthy *prometheus.GaugeVec
	// lastStateChange is the unix timestamp of the last state transition.
	lastStateChange *prometheus.GaugeVec
	// lastProbe is the unix timestamp of the last completed probe. Comparing
	// it with the current time detects stale results (stuck probes).
	lastProbe *prometheus.GaugeVec
	// probes counts completed probes by result.
	probes *prometheus.CounterVec
}

func newMetrics() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		healthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "registry_proxy",
			Subsystem: "health",
			Name:      "instance_healthy",
			Help:      "Whether the proxy instance is currently considered healthy (1) or not (0).",
		}, []string{"instance"}),
		lastStateChange: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "registry_proxy",
			Subsystem: "health",
			Name:      "instance_last_state_change_timestamp_seconds",
			Help:      "Unix timestamp of the last health state transition of the proxy instance.",
		}, []string{"instance"}),
		lastProbe: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "registry_proxy",
			Subsystem: "health",
			Name:      "instance_last_probe_timestamp_seconds",
			Help:      "Unix timestamp of the last completed probe of the proxy instance.",
		}, []string{"instance"}),
		probes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "registry_proxy",
			Subsystem: "health",
			Name:      "probes_total",
			Help:      "Total number of completed health probes by instance and result.",
		}, []string{"instance", "result"}),
	}
	m.registry.MustRegister(m.healthy, m.lastStateChange, m.lastProbe, m.probes)
	return m
}

// Handler returns an http.Handler serving the metrics of this checker.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) setState(instance string, state State, at time.Time) {
	healthy := 0.0
	if state == StateHealthy {
		healthy = 1
	}
	m.healthy.WithLabelValues(instance).Set(healthy)
	m.lastStateChange.WithLabelValues(instance).Set(float64(at.Unix()))
}

func (m *Metrics) setLastProbe(instance string, at time.Time) {
	m.lastProbe.WithLabelValues(instance).Set(float64(at.Unix()))
}

func (m *Metrics) incProbe(instance string, success bool) {
	result := "failure"
	if success {
		result = "success"
	}
	m.probes.WithLabelValues(instance, result).Inc()
}

func (m *Metrics) remove(instance string) {
	m.healthy.DeleteLabelValues(instance)
	m.lastStateChange.DeleteLabelValues(instance)
	m.lastProbe.DeleteLabelValues(instance)
	m.probes.DeleteLabelValues(instance, "success")
	m.probes.DeleteLabelValues(instance, "failure")
}

// HealthyValue returns the current value of the instance_healthy gauge.
func (m *Metrics) HealthyValue(instance string) float64 {
	return gaugeValue(m.healthy.WithLabelValues(instance))
}

// LastStateChangeValue returns the current value of the
// instance_last_state_change_timestamp_seconds gauge.
func (m *Metrics) LastStateChangeValue(instance string) float64 {
	return gaugeValue(m.lastStateChange.WithLabelValues(instance))
}

// LastProbeValue returns the current value of the
// instance_last_probe_timestamp_seconds gauge.
func (m *Metrics) LastProbeValue(instance string) float64 {
	return gaugeValue(m.lastProbe.WithLabelValues(instance))
}

// ProbesValue returns the current value of the probes_total counter for the
// given result ("success" or "failure").
func (m *Metrics) ProbesValue(instance, result string) float64 {
	return counterValue(m.probes.WithLabelValues(instance, result))
}

func gaugeValue(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		return 0
	}
	return m.GetGauge().GetValue()
}

func counterValue(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}
