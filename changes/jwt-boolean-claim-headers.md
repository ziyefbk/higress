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

# Forward boolean JWT claims to configured request headers

The JWT authentication plugin now maps boolean `claims_to_headers` values to
lowercase `true` or `false`, preserving the configured header override or append
behavior. Existing string and unsigned-integer claim handling is unchanged.

Add signed-JWT regression coverage and document boolean claim support in the
Chinese and English plugin READMEs.

References: Issue #2365.
