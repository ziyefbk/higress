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

package client

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEurekaHTTPClientRejectsFailedRequests(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if r.URL.Path == "/eureka/apps/test-service" {
					_, _ = w.Write([]byte(`{"application":{"name":"test-service","instance":[]}}`))
				} else {
					_, _ = w.Write([]byte(`{"applications":{"application":[],"versions__delta":"1","apps__hashcode":""}}`))
				}
			}))
			t.Cleanup(server.Close)
			cfg := NewDefaultConfig()
			cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
			cfg.Retries = 1
			c := NewEurekaHttpClient(cfg)
			if result, err := c.GetApplications(); err == nil || result != nil {
				t.Fatalf("failed full request returned %v, %v", result, err)
			}
			if result, err := c.GetDelta(); err == nil || result != nil {
				t.Fatalf("failed delta request returned %v, %v", result, err)
			}
			if result, err := c.GetApplication("test-service"); err == nil || result != nil {
				t.Fatalf("failed application request returned %v, %v", result, err)
			}
		})
	}
}

func TestGetApplicationsRejectsMissingEnvelope(t *testing.T) {
	for _, body := range []string{`{}`, `{"applications":null}`, `{`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			t.Cleanup(server.Close)
			cfg := NewDefaultConfig()
			cfg.BaseUrl = strings.TrimPrefix(server.URL, "http://")
			c := NewEurekaHttpClient(cfg)
			defer func() {
				if v := recover(); v != nil {
					t.Fatalf("malformed response panicked: %v", v)
				}
			}()
			if result, err := c.GetApplications(); err == nil || result != nil {
				t.Fatalf("malformed response returned %v, %v", result, err)
			}
		})
	}
}
