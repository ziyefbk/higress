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

package memory

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"istio.io/api/networking/v1alpha3"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/validation"

	ingress "github.com/alibaba/higress/v2/pkg/ingress/kube/common"
)

func endpointTestWrapper() *ingress.ServiceWrapper {
	return &ingress.ServiceWrapper{
		ServiceName: "test-service",
		Suffix:      "eureka", RegistryType: "eureka", RegistryName: "registry-a",
		ServiceEntry: &v1alpha3.ServiceEntry{
			Hosts:      []string{"test-service.eureka"},
			Ports:      []*v1alpha3.ServicePort{{Name: "http", Number: 80, Protocol: "HTTP"}},
			Resolution: v1alpha3.ServiceEntry_STATIC,
			Endpoints: []*v1alpha3.WorkloadEntry{{
				Address: "10.0.0.1", Ports: map[string]uint32{"http": 8080}, Labels: map[string]string{"version": "v1"},
			}},
		},
	}
}

func TestClearServiceWrapperEndpointsPreservesServiceAndSnapshots(t *testing.T) {
	cache := NewCache()
	original := endpointTestWrapper()
	cache.UpdateServiceWrapper("test-service.eureka", original)
	cache.ClearServiceWrapperEndpointsForRegistry("test-service.eureka", "eureka", "registry-a")

	updated, deleted := cache.GetIncrementalServiceWrapper()
	if len(updated) != 2 || len(deleted) != 0 {
		t.Fatalf("expected original and withdrawal updates, got %d updates and %d deletions", len(updated), len(deleted))
	}
	if len(original.ServiceEntry.Endpoints) != 1 || len(updated[0].ServiceEntry.Endpoints) != 1 {
		t.Fatal("endpoint withdrawal mutated a previous snapshot")
	}
	current := cache.GetAllServiceWrapper()
	if len(current) != 1 {
		t.Fatalf("expected the service identity to remain, got %v", current)
	}
	expected := original.ServiceEntry.DeepCopy()
	expected.Endpoints = nil
	if !proto.Equal(current[0].ServiceEntry, expected) || !proto.Equal(updated[1].ServiceEntry, expected) {
		t.Fatalf("withdrawal must change only endpoints, got %v", current[0].ServiceEntry)
	}
	if current[0].ServiceName != original.ServiceName || current[0].Suffix != original.Suffix || current[0].RegistryType != original.RegistryType || current[0].RegistryName != original.RegistryName || !current[0].GetCreateTime().Equal(original.GetCreateTime()) {
		t.Fatal("endpoint withdrawal changed wrapper identity or creation time")
	}
	if _, err := validation.ValidateServiceEntry(config.Config{Spec: current[0].ServiceEntry}); err != nil {
		t.Fatalf("withdrawal produced an invalid ServiceEntry: %v", err)
	}
}

func TestClearServiceWrapperEndpointsHonorsRegistryOwner(t *testing.T) {
	for _, owner := range [][2]string{{"nacos", "registry-a"}, {"eureka", "registry-b"}} {
		t.Run(owner[0]+"/"+owner[1], func(t *testing.T) {
			cache := NewCache()
			original := endpointTestWrapper()
			cache.UpdateServiceWrapper("test-service.eureka", original)
			cache.GetIncrementalServiceWrapper()
			cache.ClearServiceWrapperEndpointsForRegistry("test-service.eureka", owner[0], owner[1])
			cache.ClearServiceWrapperEndpointsForRegistry("missing.eureka", "eureka", "registry-a")
			updated, deleted := cache.GetIncrementalServiceWrapper()
			if len(updated) != 0 || len(deleted) != 0 {
				t.Fatal("missing or differently-owned services must not produce updates")
			}
			if entries := cache.GetAllServiceEntry(); len(entries) != 1 || !proto.Equal(entries[0], original.ServiceEntry) {
				t.Fatalf("modified another registry's service: %v", entries)
			}
		})
	}
}

func TestClearServiceWrapperEndpointsPreservesDeferredDeletions(t *testing.T) {
	cache := NewCache()
	cache.UpdateServiceWrapper("test-service.eureka", endpointTestWrapper())
	cache.UpdateServiceWrapper("other.eureka", endpointTestWrapper())
	cache.DeleteServiceWrapper("test-service.eureka")
	cache.DeleteServiceWrapper("other.eureka")
	cache.ClearServiceWrapperEndpointsForRegistry("test-service.eureka", "eureka", "registry-a")
	if entries := cache.GetAllServiceEntry(); len(entries) != 2 {
		t.Fatal("endpoint withdrawal must not purge shared deferred deletions")
	}
	if !cache.PurgeStaleItems() || len(cache.GetAllServiceEntry()) != 0 {
		t.Fatal("endpoint withdrawal must not cancel pending deferred deletions")
	}
}
