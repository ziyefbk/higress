// Copyright (c) 2022 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eureka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hudl/fargo"
	"istio.io/api/networking/v1alpha3"

	apiv1 "github.com/alibaba/higress/v2/api/networking/v1"
	eurekaclient "github.com/alibaba/higress/v2/registry/eureka/client"
	"github.com/alibaba/higress/v2/registry/memory"
)

const testServiceName = "test-service"

// These fixtures use the wire format returned by Eureka, so both initial
// discovery and subsequent updates exercise the production HTTP decoder.
func eurekaTestInstance(status fargo.StatusType, address string, port int, metadata map[string]interface{}) map[string]interface{} {
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	return map[string]interface{}{
		"instanceId": address,
		"app":        testServiceName,
		"ipAddr":     address,
		"status":     status,
		"port":       map[string]interface{}{"$": port, "@enabled": "true"},
		"securePort": map[string]interface{}{"$": 443, "@enabled": "false"},
		"metadata":   metadata,
	}
}

func eurekaTestResponse(t *testing.T, all bool, instances ...map[string]interface{}) []byte {
	t.Helper()
	app := map[string]interface{}{"name": testServiceName, "instance": instances}
	var response interface{} = map[string]interface{}{"application": app}
	if all {
		response = map[string]interface{}{"applications": map[string]interface{}{
			"application": []interface{}{app}, "versions__delta": "1", "apps__hashcode": "test",
		}}
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newEurekaTestWatcher(t *testing.T, server *httptest.Server, updateEmpty bool, opts ...WatcherOption) *watcher {
	t.Helper()
	opts = append(opts, WithName("test-registry"), WithType("eureka"), WithUpdateCacheWhenEmpty(updateEmpty))
	w, err := NewWatcher(memory.NewCache(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	result := w.(*watcher)
	cfg := eurekaclient.NewDefaultConfig()
	cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
	cfg.PollInterval = 3600
	cfg.Retries = 1
	result.eurekaClient = eurekaclient.NewEurekaHttpClient(cfg)
	result.AppendServiceUpdateHandler(func() {})
	result.ReadyHandler(func(bool) {})
	t.Cleanup(result.Stop)
	return result
}

func TestWatcherInitialDiscoveryFiltersInstanceStatus(t *testing.T) {
	for _, updateEmpty := range []bool{false, true} {
		t.Run(fmt.Sprintf("updateEmpty=%t", updateEmpty), func(t *testing.T) {
			for _, status := range []fargo.StatusType{fargo.DOWN, fargo.OUTOFSERVICE, fargo.UNKNOWN, fargo.STARTING, "", "UNRECOGNIZED"} {
				t.Run(string(status), func(t *testing.T) {
					body := eurekaTestResponse(t, true, eurekaTestInstance(status, "10.0.0.1", 8080, nil))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write(body)
					}))
					t.Cleanup(server.Close)
					w := newEurekaTestWatcher(t, server, updateEmpty)
					w.doFullRefresh()
					if _, subscribed := w.WatchingServices[testServiceName]; !subscribed {
						t.Fatal("inactive application must remain subscribed so recovery can be discovered")
					}
					if entries := w.cache.GetAllServiceEntry(); len(entries) != 0 {
						t.Fatalf("status %q must not create a routable service, got %v", status, entries)
					}
				})
			}
		})
	}
}

func TestWatcherMixedStatusesPreserveHealthyMetadataAndPorts(t *testing.T) {
	for _, virtualPort := range []uint32{0, 80} {
		t.Run(fmt.Sprintf("virtualPort=%d", virtualPort), func(t *testing.T) {
			instances := []map[string]interface{}{
				eurekaTestInstance(fargo.DOWN, "10.0.0.1", 8080, map[string]interface{}{"protocol": "invalid"}),
				eurekaTestInstance(fargo.OUTOFSERVICE, "10.0.0.2", 8081, nil),
				eurekaTestInstance(fargo.UNKNOWN, "10.0.0.3", 8082, nil),
				eurekaTestInstance(fargo.STARTING, "10.0.0.4", 8083, nil),
				eurekaTestInstance(fargo.UP, "10.0.0.5", 9090, map[string]interface{}{"protocol": "grpc", "version": "v1", "weight": 5}),
				eurekaTestInstance(fargo.UP, "10.0.0.6", 9091, map[string]interface{}{"protocol": "grpc", "version": "v2"}),
			}
			body := eurekaTestResponse(t, true, instances...)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			}))
			t.Cleanup(server.Close)
			w := newEurekaTestWatcher(t, server, false, WithVport(&apiv1.RegistryConfig_VPort{Default: virtualPort}))
			w.doFullRefresh()
			entries := w.cache.GetAllServiceEntry()
			if len(entries) != 1 {
				t.Fatalf("expected the healthy application, got %v", entries)
			}
			se := entries[0]
			wantPort := uint32(9090)
			if virtualPort != 0 {
				wantPort = virtualPort
			}
			if len(se.Ports) != 1 || se.Ports[0].Number != wantPort || se.Ports[0].Protocol != "GRPC" {
				t.Fatalf("unexpected service ports: %v", se.Ports)
			}
			if len(se.Endpoints) != 2 {
				t.Fatalf("expected only the two UP endpoints, got %v", se.Endpoints)
			}
			for i, endpoint := range se.Endpoints {
				wantAddress := fmt.Sprintf("10.0.0.%d", i+5)
				wantLabels := map[string]string{"protocol": "grpc", "version": fmt.Sprintf("v%d", i+1)}
				if endpoint.Address != wantAddress || endpoint.Ports["GRPC"] != uint32(9090+i) || !reflect.DeepEqual(endpoint.Labels, wantLabels) {
					t.Fatalf("UP endpoint metadata/port changed: %v", endpoint)
				}
			}
		})
	}
}

func TestWatcherPolledStatusTransitionsAndEmptyProtection(t *testing.T) {
	for _, updateEmpty := range []bool{false, true} {
		t.Run(fmt.Sprintf("updateEmpty=%t", updateEmpty), func(t *testing.T) {
			t.Parallel()
			up := eurekaTestInstance(fargo.UP, "10.0.0.1", 8080, map[string]interface{}{"version": "v1"})
			initial := eurekaTestResponse(t, true, up)
			responses := make(chan []byte)
			stopServer := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/eureka/apps" {
					_, _ = w.Write(initial)
					return
				}
				if r.URL.Path != "/eureka/apps/"+testServiceName {
					http.NotFound(w, r)
					return
				}
				select {
				case body := <-responses:
					_, _ = w.Write(body)
				case <-stopServer:
				}
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(stopServer) })
			w := newEurekaTestWatcher(t, server, updateEmpty, WithVport(&apiv1.RegistryConfig_VPort{Default: 80}))
			cfg := eurekaclient.NewDefaultConfig()
			cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
			cfg.PollInterval = 1
			cfg.Retries = 1
			w.eurekaClient = eurekaclient.NewEurekaHttpClient(cfg)
			updates := make(chan struct{}, 1)
			w.AppendServiceUpdateHandler(func() {
				select {
				case updates <- struct{}{}:
				default:
				}
			})
			waitUpdate := func() {
				t.Helper()
				select {
				case <-updates:
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for the real Eureka polling callback")
				}
			}
			poll := func(instances ...map[string]interface{}) {
				t.Helper()
				body := eurekaTestResponse(t, false, instances...)
				select {
				case responses <- body:
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for a Eureka application request")
				}
				waitUpdate()
			}
			assertUp := func(port uint32, version string) {
				t.Helper()
				entries := w.cache.GetAllServiceEntry()
				if len(entries) != 1 || len(entries[0].Endpoints) != 1 {
					t.Fatalf("expected one UP endpoint, got %v", entries)
				}
				se := entries[0]
				if se.Resolution != v1alpha3.ServiceEntry_STATIC || len(se.Ports) != 1 || se.Ports[0].Number != 80 || se.Endpoints[0].Ports["HTTP"] != port || se.Endpoints[0].Labels["version"] != version {
					t.Fatalf("unexpected recovered service entry: %v", se)
				}
			}

			w.doFullRefresh()
			waitUpdate()
			assertUp(8080, "v1")
			w.cache.GetIncrementalServiceWrapper()

			// A nonempty authoritative registry response is different from an
			// abnormal empty response, even with empty protection enabled.
			poll(eurekaTestInstance(fargo.DOWN, "10.0.0.1", 8080, nil))
			if snapshot := w.cache.GetAllServiceEntry(); len(snapshot) != 1 || len(snapshot[0].Endpoints) != 0 {
				t.Fatalf("DOWN endpoint remains visible without a McpBridge reconcile: %v", snapshot)
			}
			updated, deleted := w.cache.GetIncrementalServiceWrapper()
			if len(updated) != 1 || len(deleted) != 0 {
				t.Fatalf("DOWN must publish an empty endpoint update, got %d updates and %d deletions", len(updated), len(deleted))
			}
			if len(updated[0].ServiceEntry.Endpoints) != 0 {
				t.Fatalf("DOWN endpoint remains in incremental update: %v", updated[0].ServiceEntry)
			}
			// Ordinary polling callbacks do not reconcile McpBridge or purge
			// deferred deletions. The xDS snapshot must already have no endpoints.
			entries := w.cache.GetAllServiceWrapper()
			if len(entries) != 1 || len(entries[0].ServiceEntry.Endpoints) != 0 {
				t.Fatalf("DOWN endpoint remains in the published snapshot: %v", entries)
			}
			if ports := entries[0].ServiceEntry.Ports; len(ports) != 1 || ports[0].Number != 80 || ports[0].Protocol != "HTTP" {
				t.Fatalf("endpoint withdrawal must preserve the valid service port: %v", ports)
			}

			up = eurekaTestInstance(fargo.UP, "10.0.0.1", 9090, map[string]interface{}{"version": "v2"})
			poll(up)
			assertUp(9090, "v2")
			w.cache.GetIncrementalServiceWrapper()

			poll()
			updated, deleted = w.cache.GetIncrementalServiceWrapper()
			wantDeleted := 0
			if updateEmpty {
				wantDeleted = 1
			}
			if len(updated) != 0 || len(deleted) != wantDeleted {
				t.Fatalf("empty response: got %d updates and %d deletions, want 0 and %d", len(updated), len(deleted), wantDeleted)
			}
			// Preserve the existing empty-response policy: optional deferred
			// deletion, finalized by the McpBridge reconciler rather than polling.
			assertUp(9090, "v2")

			poll(up)
			assertUp(9090, "v2")
		})
	}
}
