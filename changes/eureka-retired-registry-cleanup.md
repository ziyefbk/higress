<!--
Copyright 2026 alibaba

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Clean up retained Eureka services when their registry stops

Retire all service cache entries still owned by a stopped Eureka registry,
including protected entries whose subscriptions have disappeared. Serialize
polling callbacks with retirement, reject obsolete Plan updates, and prevent
late full refreshes from restoring services or readiness after Stop.

Keep application updates responsive while a full-registry HTTP request is
stalled. Reject non-successful HTTP responses and missing applications envelopes
instead of treating them as service disappearance or panicking.

The existing empty-response protection for an active registry is unchanged.

Related: Issue #2896; empty-protection and registry-removal policy in Issue #1546.
