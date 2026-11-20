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

package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/ketches/registry-proxy/internal/config"
	"github.com/ketches/registry-proxy/internal/global"
	"github.com/ketches/registry-proxy/internal/health"
	"github.com/ketches/registry-proxy/internal/mutate"
	"github.com/ketches/registry-proxy/pkg/util"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// metricsAddr is the plain-HTTP address the metrics endpoint is served on.
const metricsAddr = ":8080"

func init() {
	Init()
}

// Run starts the registry-proxy admission webhook.
func Run() {
	startHealthChecking()

	http.HandleFunc(global.WebhookServicePath, mutatePod)
	log.Println("Start serving registry-proxy admission webhook ...")

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			health.Default().Metrics().Handler().ServeHTTP(w, r)
		})
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("Failed to serve metrics endpoint: %v", err)
		}
	}()

	if err := http.ListenAndServeTLS(":443", global.WebhookServiceTLSCertFile, global.WebhookServiceTLSKeyFile, nil); err != nil {
		log.Fatalln("Failed to listen and serve admission webhook.", err.Error())
	}
}

// startHealthChecking starts background health checking of the proxy
// instances referenced by the current rules, and keeps the probe set in sync
// with rule hot-reloads.
func startHealthChecking() {
	syncHealthFromConfig()
	config.RegisterOnReset(syncHealthFromConfig)
}

// syncHealthFromConfig applies the current config to the default health checker.
func syncHealthFromConfig() {
	cfg, err := config.GetHealthCheck().Parse()
	if err != nil {
		// Should not happen: invalid configs are rejected by config.Reset.
		log.Printf("Invalid health check config, health gating unchanged: %v", err)
		return
	}
	if err := health.ReconfigureDefault(cfg, uniqueProxyAddrs(config.GetProxies())); err != nil {
		log.Printf("Failed to apply health check config: %v", err)
	}
}

// uniqueProxyAddrs returns the distinct proxy addresses referenced by the rules.
func uniqueProxyAddrs(proxies map[string]string) []string {
	seen := map[string]struct{}{}
	var addrs []string
	for _, addr := range proxies {
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		addrs = append(addrs, addr)
	}
	return addrs
}

// mutatePod is the handler of the admission webhook.
func mutatePod(w http.ResponseWriter, r *http.Request) {
	log.Println("Request admission webhook mutating ...")

	request, pod, err := parseRequest(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not parse request: %v", err), http.StatusBadRequest)
		return
	}

	// If the pod not match the pod selector, return directly.
	if selector := config.PodSelector().AsSelector(); !selector.Empty() {
		if !selector.Matches(labels.Set(pod.Labels)) {
			response(w, request, nil)
			return
		}
	}

	patchBytes, err := patchPod(pod)
	if err != nil {
		log.Println("Marshal patch failed.")
		http.Error(w, fmt.Sprintf("could not marshal patch: %v", err), http.StatusInternalServerError)
		return
	}

	response(w, request, patchBytes)
}

// parseRequest parses the request of the admission webhook.
func parseRequest(r *http.Request) (*admissionv1.AdmissionReview, *corev1.Pod, error) {
	var (
		request admissionv1.AdmissionReview
		pod     = &corev1.Pod{}
	)

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.Println("Decode body failed.")
		return nil, nil, fmt.Errorf("could not decode body: %v", err)
	}

	raw := request.Request.Object.Raw

	if err := json.Unmarshal(raw, pod); err != nil {
		log.Println("Unmarshal pod object failed.", err.Error())
		return nil, nil, fmt.Errorf("could not unmarshal pod object: %v", err)
	}

	return &request, pod, nil
}

// patchPod generates the patch for the pod.
func patchPod(pod *corev1.Pod) ([]byte, error) {
	// If the pod is controlled by a controller, set podName as generateName.
	podName := util.ValueIf(pod.Name != "", pod.Name, pod.GenerateName)

	log.Printf("Pod %s/%s is included", pod.Namespace, podName)

	mutate.Pod(pod)

	var patches = []map[string]any{
		{
			"op":    "replace",
			"path":  "/spec/initContainers",
			"value": pod.Spec.InitContainers,
		},
		{
			"op":    "replace",
			"path":  "/spec/containers",
			"value": pod.Spec.Containers,
		},
	}

	return json.Marshal(patches)
}

// response sends the response to the admission webhook.
func response(w http.ResponseWriter, request *admissionv1.AdmissionReview, patchBytes []byte) {
	response := &admissionv1.AdmissionReview{
		TypeMeta: request.TypeMeta,
		Response: &admissionv1.AdmissionResponse{
			UID:     request.Request.UID,
			Allowed: true,
			Result:  nil,
		},
	}

	if len(patchBytes) > 0 {
		response.Response.Patch = patchBytes
		response.Response.PatchType = func() *admissionv1.PatchType {
			pt := admissionv1.PatchTypeJSONPatch
			return &pt
		}()
	}

	encodeResponse(w, response)
}

// encodeResponse encodes the response to the admission webhook.
func encodeResponse(w http.ResponseWriter, response *admissionv1.AdmissionReview) {
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println("Encode response failed.")
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
	}
}
