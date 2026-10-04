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
	ingress "github.com/alibaba/higress/v2/pkg/ingress/kube/common"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eurekaclient "github.com/alibaba/higress/v2/registry/eureka/client"
	"github.com/alibaba/higress/v2/registry/memory"
)

const lifecycleApplicationJSON = `{"name":"test-service","instance":[{"instanceId":"instance-1","app":"test-service","ipAddr":"10.0.0.1","status":"UP","port":{"$":8080,"@enabled":"true"},"securePort":{"$":443,"@enabled":"false"},"metadata":{}}]}`
const lifecycleAppsJSON = `{"applications":{"application":[` + lifecycleApplicationJSON + `],"versions__delta":"1","apps__hashcode":"UP_1_"}}`
const lifecycleEmptyAppsJSON = `{"applications":{"application":[],"versions__delta":"2","apps__hashcode":""}}`

func newLifecycleWatcher(t *testing.T, server *httptest.Server) *watcher {
	t.Helper()
	result, err := NewWatcher(memory.NewCache(), WithName("registry-a"), WithType("eureka"))
	if err != nil {
		t.Fatal(err)
	}
	w := result.(*watcher)
	cfg := eurekaclient.NewDefaultConfig()
	cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
	cfg.PollInterval = 3600
	cfg.Retries = 1
	w.eurekaClient = eurekaclient.NewEurekaHttpClient(cfg)
	w.AppendServiceUpdateHandler(func() {})
	w.ReadyHandler(func(bool) {})
	t.Cleanup(func() {
		if !w.isStop {
			w.Stop()
		}
	})
	return w
}

func TestWatcherStopRemovesPreviouslyDisappearedServices(t *testing.T) {
	var disappeared atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if disappeared.Load() {
			_, _ = w.Write([]byte(lifecycleEmptyAppsJSON))
		} else {
			_, _ = w.Write([]byte(lifecycleAppsJSON))
		}
	}))
	t.Cleanup(server.Close)
	w := newLifecycleWatcher(t, server)
	w.doFullRefresh()
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 1 {
		t.Fatalf("initial discovery failed: %v", entries)
	}
	w.cache.GetIncrementalServiceWrapper()
	disappeared.Store(true)
	w.doFullRefresh()
	// Default empty-response protection is intentional. Preserve it while the
	// registry is active, but it must not orphan cache entries when stopped.
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 1 {
		t.Fatalf("empty protection changed: %v", entries)
	}
	w.Stop()
	_, deleted := w.cache.GetIncrementalServiceWrapper()
	if len(deleted) != 1 {
		t.Errorf("Stop must queue cleanup for previously-disappeared owned service, got %d deletions", len(deleted))
	}
	// Stop is called by McpBridge reconciliation, which finalizes these deletions.
	w.cache.PurgeStaleItems()
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 0 {
		t.Fatalf("stopped registry leaked previously-disappeared service: %v", entries)
	}
}

func TestWatcherFailedRefreshPreservesSubscriptions(t *testing.T) {
	for _, body := range []string{lifecycleEmptyAppsJSON, `{"message":"maintenance"}`} {
		t.Run(body, func(t *testing.T) {
			var failed atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if failed.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(body))
				} else {
					_, _ = w.Write([]byte(lifecycleAppsJSON))
				}
			}))
			t.Cleanup(server.Close)
			w := newLifecycleWatcher(t, server)
			w.doFullRefresh()
			failed.Store(true)
			func() {
				defer func() {
					if v := recover(); v != nil {
						t.Errorf("HTTP failure panicked: %v", v)
					}
				}()
				w.doFullRefresh()
			}()
			if len(w.WatchingServices) != 1 {
				t.Fatal("HTTP failure was interpreted as service disappearance")
			}
			if entries := w.cache.GetAllServiceEntry(); len(entries) != 1 || len(entries[0].Endpoints) != 1 {
				t.Fatalf("HTTP failure changed healthy cache: %v", entries)
			}
		})
	}
}

type lifecycleBlockingCache struct {
	memory.Cache
	blockNext atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	written   chan struct{}
}

func (c *lifecycleBlockingCache) UpdateServiceWrapper(name string, entry *ingress.ServiceWrapper) {
	if c.blockNext.Swap(false) {
		close(c.entered)
		<-c.release
		c.Cache.UpdateServiceWrapper(name, entry)
		close(c.written)
		return
	}
	c.Cache.UpdateServiceWrapper(name, entry)
}

func TestWatcherStopDrainsInFlightPollingCallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/eureka/apps" {
			_, _ = w.Write([]byte(lifecycleAppsJSON))
		} else {
			_, _ = w.Write([]byte(`{"application":` + lifecycleApplicationJSON + `}`))
		}
	}))
	t.Cleanup(server.Close)
	w := newLifecycleWatcher(t, server)
	cfg := eurekaclient.NewDefaultConfig()
	cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
	cfg.PollInterval = 1
	cfg.Retries = 1
	w.eurekaClient = eurekaclient.NewEurekaHttpClient(cfg)
	c := &lifecycleBlockingCache{Cache: w.cache, entered: make(chan struct{}), release: make(chan struct{}), written: make(chan struct{})}
	w.cache = c
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(c.release) }) }
	defer release()
	w.doFullRefresh()
	c.blockNext.Store(true)
	select {
	case <-c.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("poll callback never entered cache")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	purged := false
	select {
	case <-stopped:
		// Reconciler can purge immediately after Stop returns, while the old callback is still paused.
		c.PurgeStaleItems()
		purged = true
	case <-time.After(100 * time.Millisecond):
		// A correct Stop may wait for the existing callback; let it finish.
	}
	release()
	select {
	case <-c.written:
	case <-time.After(5 * time.Second):
		t.Fatal("poll callback did not complete")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("registry Stop did not complete")
	}
	if !purged {
		c.PurgeStaleItems()
	}
	if got := c.GetAllServiceEntry(); len(got) != 0 {
		t.Fatalf("callback restored stopped registry endpoints after completed cleanup: %v", got)
	}
}

func TestWatcherStalledFullRefreshDoesNotBlockUpdatesOrStop(t *testing.T) {
	var fullRequests atomic.Int32
	fullStarted := make(chan struct{})
	releaseFull := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFull) }) }
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/eureka/apps" {
			if fullRequests.Add(1) == 2 {
				close(fullStarted)
				<-releaseFull
			}
			_, _ = w.Write([]byte(lifecycleAppsJSON))
			return
		}
		_, _ = w.Write([]byte(`{"application":` + strings.Replace(lifecycleApplicationJSON, "8080", "9090", 1) + `}`))
	}))
	t.Cleanup(server.Close)
	w := newLifecycleWatcher(t, server)
	cfg := eurekaclient.NewDefaultConfig()
	cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
	cfg.PollInterval = 1
	cfg.Retries = 1
	w.eurekaClient = eurekaclient.NewEurekaHttpClient(cfg)
	updates := make(chan struct{}, 2)
	w.AppendServiceUpdateHandler(func() {
		select {
		case updates <- struct{}{}:
		default:
		}
	})
	w.doFullRefresh()
	<-updates
	refreshed := make(chan struct{})
	go func() { w.doFullRefresh(); close(refreshed) }()
	select {
	case <-fullStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("full request never started")
	}
	select {
	case <-updates:
		entries := w.cache.GetAllServiceEntry()
		if len(entries) != 1 || len(entries[0].Endpoints) != 1 || entries[0].Endpoints[0].Ports["HTTP"] != 9090 {
			t.Errorf("poll did not publish while full refresh was stalled: %v", entries)
		}
	case <-time.After(5 * time.Second):
		t.Error("stalled full refresh blocked application updates")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Error("stalled full refresh blocked Stop")
	}
	release()
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("full refresh did not finish")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not finish")
	}
	w.cache.PurgeStaleItems()
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 0 {
		t.Fatalf("late full refresh restored a stopped registry: %v", entries)
	}
}

func TestWatcherStopPreventsLaterRefreshAndIsIdempotent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(lifecycleAppsJSON))
	}))
	t.Cleanup(server.Close)
	w := newLifecycleWatcher(t, server)
	w.doFullRefresh()
	w.Stop()
	w.cache.PurgeStaleItems()
	w.doFullRefresh()
	// The baseline can accidentally create a Plan after stopping; clean that
	// Plan even on the expected red run without closing the watcher twice.
	defer func() {
		for _, plan := range w.WatchingServices {
			plan.Stop()
		}
	}()
	if got := requests.Load(); got != 1 {
		t.Errorf("stopped watcher issued %d registry requests, want 1", got)
	}
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 0 {
		t.Errorf("stopped watcher repopulated cache: %v", entries)
	}
	defer func() {
		if v := recover(); v != nil {
			t.Errorf("repeated Stop panicked: %v", v)
		}
	}()
	w.Stop()
}

func TestWatcherStopDuringInitialFetchCannotBecomeReady(t *testing.T) {
	started := make(chan struct{})
	releaseFetch := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFetch) }) }
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-releaseFetch
		_, _ = w.Write([]byte(lifecycleAppsJSON))
	}))
	t.Cleanup(server.Close)
	w := newLifecycleWatcher(t, server)
	ready := make(chan bool, 4)
	w.ReadyHandler(func(value bool) { ready <- value })
	runDone := make(chan struct{})
	go func() { w.Run(); close(runDone) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("initial registry fetch did not start")
	}
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Error("initial fetch blocked Stop")
	}
	release()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not complete")
	}
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	close(ready)
	sawStop := false
	for value := range ready {
		if !value {
			sawStop = true
		} else if sawStop {
			t.Error("watcher became ready after Stop")
		}
	}
	if w.IsReady() {
		t.Error("stopped watcher must remain unready")
	}
	w.cache.PurgeStaleItems()
	if entries := w.cache.GetAllServiceEntry(); len(entries) != 0 {
		t.Fatalf("initial fetch restored cache after Stop: %v", entries)
	}
}
