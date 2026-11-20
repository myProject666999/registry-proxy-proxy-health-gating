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

// Package mutate rewrites pod container images to their proxy addresses,
// gated by the health of the target proxy instance.
package mutate

import (
	"log"
	"path"

	"github.com/ketches/registry-proxy/internal/config"
	"github.com/ketches/registry-proxy/internal/health"
	"github.com/ketches/registry-proxy/pkg/image"
	corev1 "k8s.io/api/core/v1"
)

// Pod replaces the images of all containers of the pod with their proxy
// images, unless the responsible proxy instance is not healthy.
func Pod(pod *corev1.Pod) {
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Image = proxyImage(pod.Spec.InitContainers[i].Image)
	}

	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Image = proxyImage(pod.Spec.Containers[i].Image)
	}

	for i := range pod.Spec.EphemeralContainers {
		pod.Spec.EphemeralContainers[i].EphemeralContainerCommon.Image = proxyImage(pod.Spec.EphemeralContainers[i].EphemeralContainerCommon.Image)
	}
}

// proxyImage gets the proxy image of the raw image.
func proxyImage(rawImage string) string {
	var result = rawImage
	registry, name, err := image.Parse(rawImage)
	if err != nil {
		log.Println("Parse image failed.")
		return result
	}

	result = path.Join(proxyRegistry(registry), name)
	if result != rawImage {
		log.Printf("Proxy image: %s -> %s", rawImage, result)
	}
	return result
}

// proxyRegistry gets the proxy registry of the raw registry. When the proxy
// instance responsible for the registry is not known to be healthy, the raw
// registry is kept so that the workload pulls from the original address.
func proxyRegistry(rawRegistry string) string {
	proxies := config.GetProxies()
	if proxies == nil {
		return rawRegistry
	}
	newRegistry := proxies[rawRegistry]
	if newRegistry == "" {
		return rawRegistry
	}
	if !health.Default().ShouldRewrite(newRegistry) {
		log.Printf("Proxy instance %s is not healthy (or unknown), keep original registry %s", newRegistry, rawRegistry)
		return rawRegistry
	}
	return newRegistry
}
