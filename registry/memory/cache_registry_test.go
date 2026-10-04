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
	ingress "github.com/alibaba/higress/v2/pkg/ingress/kube/common"
	"istio.io/api/networking/v1alpha3"
	"testing"
)

func registryOwnedWrapper(host, registryType, registryName string) *ingress.ServiceWrapper {
	return &ingress.ServiceWrapper{RegistryType: registryType, RegistryName: registryName, ServiceEntry: &v1alpha3.ServiceEntry{Hosts: []string{host}, Endpoints: []*v1alpha3.WorkloadEntry{{Address: "10.0.0.1"}}}}
}

func TestDeleteServiceWrappersForRegistryPreservesOtherOwners(t *testing.T) {
	c := NewCache()
	c.UpdateServiceWrapper("active", registryOwnedWrapper("active", "eureka", "a"))
	c.UpdateServiceWrapper("protected", registryOwnedWrapper("protected", "eureka", "a"))
	c.UpdateServiceWrapper("other-name", registryOwnedWrapper("other-name", "eureka", "b"))
	c.UpdateServiceWrapper("other-type", registryOwnedWrapper("other-type", "nacos", "a"))
	c.GetIncrementalServiceWrapper()
	c.DeleteServiceWrapper("active")
	c.DeleteServiceWrappersForRegistry("eureka", "a")
	c.DeleteServiceWrappersForRegistry("eureka", "a")
	updated, deleted := c.GetIncrementalServiceWrapper()
	if len(updated) != 0 || len(deleted) != 2 {
		t.Fatalf("want exactly two owned deletions, got %d updates and %d deletions", len(updated), len(deleted))
	}
	if len(c.GetAllServiceEntry()) != 4 {
		t.Fatal("retirement must remain deferred until reconciliation")
	}
	c.PurgeStaleItems()
	entries := c.GetAllServiceEntry()
	if len(entries) != 2 {
		t.Fatalf("deleted another registry's entries: %v", entries)
	}
	for _, entry := range entries {
		if entry.Hosts[0] != "other-name" && entry.Hosts[0] != "other-type" {
			t.Fatalf("owned service survived retirement: %v", entry)
		}
	}
}

func TestDeleteServiceWrappersForRegistryAllowsReplacement(t *testing.T) {
	c := NewCache()
	c.UpdateServiceWrapper("shared", registryOwnedWrapper("shared", "eureka", "a"))
	c.DeleteServiceWrappersForRegistry("eureka", "a")
	c.UpdateServiceWrapper("shared", registryOwnedWrapper("shared", "eureka", "b"))
	c.DeleteServiceWrappersForRegistry("eureka", "a")
	c.PurgeStaleItems()
	entries := c.GetAllServiceWrapper()
	if len(entries) != 1 || entries[0].RegistryName != "b" {
		t.Fatalf("replacement owner was deleted: %v", entries)
	}
}
