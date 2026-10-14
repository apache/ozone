<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Ganesha integration

The FSAL requires the caller's authenticated RPCSEC_GSS identity. Upstream Ganesha V5.7 keeps that identity on the RPC request, while FSAL callbacks receive only the mapped numeric credentials in `req_op_context`. The patch here adds bounded principal/mechanism fields and the GSS protection level to the callback context. It increments the FSAL API minor version as required when appending context fields.

Apply `patches/0001-expose-rpcsec-gss-client-context.patch`, `patches/0003-capture-rpcsec-gss-context-per-nfs4-compound.patch`, `patches/0002-add-ozone-fsal-build-target.patch`, and `patches/0004-include-assert-header-in-ntirpc.patch` to a clean checkout at commit `3113f970511289ae8928078cf392d17c0df81b38` (tag `V5.7`), with its pinned `src/libntirpc` submodule initialized. Copy this directory's `FSAL_OZONE` contents into that checkout's `src/FSAL/FSAL_OZONE` directory. The first patch adds bounded identity fields to the FSAL callback context. The third patch captures the authenticated RPCSEC_GSS principal and protection level for every NFSv4 COMPOUND before dispatch; it does not expose the live `svc_req`, `SVCAUTH`, or GSS context pointer to FSAL code. It also keeps the negotiated mechanism OID separate from the caller-name type OID returned by `gss_display_name`, so the FSAL can validate Kerberos requests correctly. This preserves identity for read operations that do not invoke `nfs_req_creds`, rejects missing or oversized values, and preserves principal bytes with an explicit length. The second patch adds the optional `USE_FSAL_OZONE` CMake target; the fourth fixes a missing `assert.h` include in pinned ntirpc that fails with GCC 13's implicit-function-declaration error.

Configure with `-DUSE_FSAL_OZONE=ON` and `-DOZONE_NFS_GATEWAY_SOURCE=<path-to-ozone-nfs-gateway>`, then build the `fsalozone` target. The Ozone gateway directory must also have its optional C++ gRPC dependencies installed. The export requires an operator-pinned export UUID, volume and bucket IDs, mutual-TLS files, and a backend advertising GETATTR, LOOKUP, READDIR, READ, CREATE, MKDIR, REMOVE, and RENAME. The FSAL handles NFSv4 OPEN, reopen, status, and close state and dispatches guarded file creation, directory creation, removal, rename, and bounded writes through the Java gateway. WRITE ranges are durable staging objects; COMMIT publishes them into the existing canonical Ozone file, and final close removes range manifests and staging objects before releasing the lease. It rejects AUTH_SYS, non-Kerberos GSS mechanisms, embedded-NUL or oversized principals, and protection levels other than integrity or privacy before asserting a caller to the Java gateway. The patches are integration inputs, not vendored Ganesha source. Build and test the plugin against the patched source and its pinned ntirpc submodule; successful linking does not replace mounted NFS contract tests.
