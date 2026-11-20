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
	"fmt"
	"net/http"
	"strings"
	"time"
)

// manifestAcceptHeader advertises the manifest media types a probe accepts.
const manifestAcceptHeader = "application/vnd.oci.image.index.v1+json, " +
	"application/vnd.oci.image.manifest.v1+json, " +
	"application/vnd.docker.distribution.manifest.v2+json, " +
	"application/vnd.docker.distribution.manifest.list.v2+json"

// userAgent identifies health probe requests to the proxy.
const userAgent = "registry-proxy-healthcheck"

// HTTPProbe returns a ProbeFunc that probes a proxy instance by issuing a
// HEAD request for the manifest of probeImage ("repository:tag", without
// registry host) against the instance.
//
// A manifest request is used instead of a bare TCP connect because a proxy
// whose port accepts connections but cannot serve registry requests is far
// more common than a fully dead one. HEAD (not GET) avoids transferring the
// manifest body, and manifests are small and typically cached by the proxy,
// so probing adds negligible load on the proxy and almost never reaches the
// upstream registry (and therefore does not count against upstream rate
// limits in any meaningful way).
//
// addr may be a bare host (https is assumed) or a full URL with scheme
// (useful for tests against plain-HTTP servers).
func HTTPProbe(probeImage string, timeout time.Duration) ProbeFunc {
	repo, tag := splitProbeImage(probeImage)
	client := &http.Client{Timeout: timeout}
	return func(ctx context.Context, addr string) error {
		base := addr
		if !strings.Contains(base, "://") {
			base = "https://" + base
		}
		url := fmt.Sprintf("%s/v2/%s/manifests/%s", strings.TrimSuffix(base, "/"), repo, tag)
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", manifestAcceptHeader)
		req.Header.Set("User-Agent", userAgent)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status %s", resp.Status)
		}
		return nil
	}
}

// splitProbeImage splits "repository:tag" into its parts. A missing tag
// defaults to "latest".
func splitProbeImage(probeImage string) (repo, tag string) {
	repo, tag = probeImage, "latest"
	if i := strings.LastIndex(probeImage, ":"); i > 0 {
		repo, tag = probeImage[:i], probeImage[i+1:]
	}
	return repo, tag
}
