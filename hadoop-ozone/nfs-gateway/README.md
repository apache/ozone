<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# NFS gateway development

This directory contains the Java mutual-TLS listener and peer/export authorization, a versioned backend proto, an optional native C++ gRPC adapter, native file-handle codecs, and transport/mounted-file contracts. The FSAL_OZONE module implements GETATTR, LOOKUP, READDIR, bounded READ, CREATE, MKDIR, REMOVE, RENAME, and NFSv4 write callbacks against patched Ganesha V5.7. The gateway can publish durable staged ranges into the canonical Ozone file under an OM lease fence, then clean up ranges on final close. Linux/aarch64 NFSv4.1 `sec=krb5p` mounted contracts pass read operations, file create/write/fsync/COMMIT/rename/close/remove, and directory create/remove against the Java fixture over mutual TLS. The fixture does not persist data in Ozone; the secure-cluster publication contract separately verifies OzoneFS readback after mounted NFS writes. Mounted host-loss recovery remains pending. The gateway advertises these capabilities so the FSAL can reject incomplete backends at export startup.

The `nfs-native` GitHub Actions workflow builds the Java gateway and pinned Ganesha FSAL, runs the gateway and C++/Java mutual-TLS tests, and executes `TestNfsCallerAuthorizationSecure` against an HA MiniOzoneCluster and MiniKDC. Hosted run [38621762604](https://github.com/jojochuang/ozone/actions/runs/38621762604) passed on commit `edbba6cb6f2`, including Java gateway unit tests, the pinned Ganesha V5.7/FSAL build, secure mounted NFSv4.1 `sec=krb5p` publication and COMMIT, S3/NFS writer-lease integration, native handle and C++/Java mutual-TLS contracts, mounted-runner checks, and the Ganesha export capability handshake. The workflow activates Ozone's existing `hadoop-native-lib` profile for the secure integration test; this supplies `libhadoop` to the forked test JVM and resolved the hosted mounted COMMIT stall. The other mounted Linux contracts described below run separately on provisioned clients. Shared `RecoveryRoot` over separate physical hosts and representative performance remain unverified.

The mounted OzoneFS publication contract also runs a GNU `cp` upload from local storage to the NFS mount and compares the mounted and OzoneFS bytes. It asserts that `chmod` is rejected with `EOPNOTSUPP` or `EPERM` while the file data stays unchanged; `EPERM` is expected when root squashing prevents the caller from owning the file. On 2026-10-13, all four publication tests passed on a secure Linux NFSv4.1 `sec=krb5p` mount, including byte-exact readback of EC and KMS-encrypted keys. Metadata-preserving upload clients such as `cp -p` need a separate compatibility decision and test.

## Stable export configuration

The Java binding loader accepts named properties in the gateway's persisted Ozone configuration. For an export named `data`, all six properties are required:

```properties
ozone.nfs.export.data.uuid=00112233-4455-6677-8899-aabbccddeeff
ozone.nfs.export.data.cluster.id=<expected Ozone cluster ID>
ozone.nfs.export.data.volume=<volume name>
ozone.nfs.export.data.bucket=<bucket name>
ozone.nfs.export.data.volume.id=<native volume ID in unsigned decimal>
ozone.nfs.export.data.bucket.id=<native bucket ID in unsigned decimal>
```

Choose the UUID once when creating an export and retain it with the exact cluster and bucket incarnation across gateway replacement. The example UUID is illustrative. Remapping an export requires a new UUID; startup never generates one automatically. IDs use unsigned decimal notation even when their Java `long` representation is negative. Export configuration names contain only letters, digits, underscores, and hyphens.

`NfsExportBinding.fromConfiguration` rejects missing properties, invalid UUIDs, invalid unsigned IDs, and ambiguous configuration names. Handle validation requires the actual connected cluster ID from trusted backend discovery and rejects another export or incarnation. OM still validates live metadata and checks caller ACLs. Loading these properties does not start a gateway or grant access; trusted cluster discovery and an export-ready capability handshake remain prerequisites.

## Gateway process

The distribution includes the Java backend and exposes it as `ozone nfs`. After building a distribution, run the package smoke test to verify the gateway jar, classpath, and launcher. The launcher reaches the secure-mode check when invoked without secure configuration. It requires Ozone secure mode, a service Kerberos principal and keytab, a readable server certificate/private key/CA bundle, and at least one Ganesha client certificate SHA-256 fingerprint. Configure the listener to a loopback address; it rejects non-loopback binds. After setting the gateway and export properties above, start it with:

```sh
python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_distribution.py \
  hadoop-ozone/dist/target/ozone-*-SNAPSHOT
```

Set the listener identity and TLS trust explicitly in `ozone-site.xml`. Fingerprints are 64 hexadecimal SHA-256 characters without colons; the server certificate must be valid for the hostname configured in the Ganesha FSAL.

```properties
ozone.security.enabled=true
ozone.nfs.gateway.address=127.0.0.1
ozone.nfs.gateway.port=9878
ozone.nfs.gateway.kerberos.principal=nfs-gateway/_HOST@EXAMPLE.COM
ozone.nfs.gateway.kerberos.keytab.file=/etc/security/keytabs/nfs-gateway.keytab
ozone.nfs.gateway.server.certificate.file=/etc/ozone/nfs-gateway.crt
ozone.nfs.gateway.server.private.key.file=/etc/ozone/nfs-gateway.key
ozone.nfs.gateway.trusted.ca.file=/etc/ozone/ganesha-client-ca.crt
ozone.nfs.gateway.ganesha.cert.sha256=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

The gateway registers an `NfsGatewayMetrics` source with Ozone Metrics2. For each backend RPC it reports request count, average latency in nanoseconds, NFS error responses, and gRPC failures using metric names such as `ReadRequests`, `ReadLatencyNsNumOps`, `ReadLatencyNsAvgTime`, `ReadNfsErrors`, and `ReadRpcErrors`. Metrics are per operation and do not include caller or export identifiers.

The gateway service principal needs proxy-user rules for NFS callers and permissions for export lease operations. Create the private staging bucket and configure its volume and bucket separately before enabling writes. Mounted writes, Ozone publication, same-gateway restart/reclaim, and changed-hostname replacement-container reclaim using persisted Ganesha recovery state pass against a real Ozone export. Separate physical hosts and remote shared-storage recovery remain production gates.

The gateway checks its keytab login for a renewable TGT every minute and calls Hadoop UGI's `checkTGTAndReloginFromKeytab`; the check stops when the gateway server closes. This keeps the gateway's own OM credentials renewable during long-running service. A live KDC test that renews the gateway TGT during an NFS workload remains part of production acceptance.

The gateway also requires a stable, unique `ozone.nfs.gateway.id`. It acquires an OM-owned lease for every configured export before listening, renews each lease every third of its configured duration, and stops serving an export immediately after a renewal failure. On clean shutdown it releases its leases. The default duration is 30 seconds; `ozone.nfs.gateway.export.lease.duration` accepts durations from 3 seconds through 60 seconds. The gateway service principal must have write access to each exported bucket for lease operations.

Durable range staging uses a separate operator-created native FSO bucket. Configure both names, keep the bucket outside every exported namespace, and grant the gateway service principal read/write access. Do not expose this bucket to NFS, OzoneFS, or S3 users. Staged objects and published files use their bucket's default replication, falling back to the gateway's configured RATIS factor and then Ozone's RATIS/THREE default. NFS writes fail closed for EC replication. The staging helper writes and closes an immutable range object before the OM manifest can reference it; close commits the object with the configured replication. Java WRITE dispatch persists each range before returning UNSTABLE; COMMIT publishes those ranges into the canonical file, and the gateway recovers a lease from the persistent file handle after restart. The lease owner reads staged ranges until OM records that the canonical file commit succeeded; other callers read the currently published canonical file subject to normal Ozone ACLs. After the published marker is set, reads use the canonical file even if cleanup left range manifests behind. COMMIT assembles staged data into the existing canonical file under the current OM fence; final CLOSE removes range manifests and staged objects before releasing the lease. A secured HA integration test kills the gateway JVM after staging a file, then verifies a replacement gateway recovers and publishes byte-exact data. Mounted writes and publication pass against the secure cluster. On 2026-10-14, a Linux NFSv4.1 client also reclaimed an open file across replacement Ganesha containers with different hostnames, the same NFS address and explicit server identity, distinct gateway IDs, and a shared local bind-mounted `RecoveryRoot`; the replacement gateway acquired its OM lease after expiry, and read/write/fsync completed with byte-exact final contents. Separate physical hosts and remote shared-storage recovery remain unverified.

```properties
ozone.nfs.gateway.id=nfs-gateway-01
ozone.nfs.gateway.export.lease.duration=30s
ozone.nfs.gateway.staging.volume=nfs-system
ozone.nfs.gateway.staging.bucket=write-staging
```

When replacing a gateway, use a distinct ID for the replacement process. OM permits a new gateway to take over after the previous export lease expires.

The pinned Ganesha server configuration example is [ganesha.conf.example](src/main/ganesha/ganesha.conf.example). It enables only NFSv4.1, requires Kerberos privacy, and keeps Ganesha's recovery database in `RecoveryRoot`. Create that directory before starting Ganesha and make it writable by the Ganesha service account. For fenced active/passive replacement, both hosts must use the same durable `RecoveryRoot`, `Clustered = true`, the same explicit Ganesha node ID (`ganesha.nfsd -I 0`), export identity, NFS address, `Server_Scope`, and `Server_Owner`. Keep exactly one instance active; `fs_ng` does not provide active/active recovery. The Java gateway is loopback-only and owns the OM export lease, so it must also start on the replacement host and acquire the lease before that host serves requests. On 2026-10-14, the secure mounted contract passed across containers with different hostnames: it reclaimed an open file and verified read, append, fsync, and byte-exact contents after replacement. The replacement gateway waited for the prior OM export lease to expire before it started serving. This used a shared bind-mounted `RecoveryRoot` on one Docker host; separate physical hosts and remote shared-storage behavior remain production gates. The pinned V5.7 source maps the shared recovery path to `RecoveryRoot/RecoveryDir/node0` for this configuration. Automatic failover remains unsupported.

### Separate-host recovery acceptance

Run this gate on a disposable Linux client and two separate gateway hosts. The NFS client must keep one NFSv4.1 `sec=krb5p` mount at the same address through the transition. Both gateway hosts must use the same durable `RecoveryRoot` and the same Ganesha export/server identity; the replacement uses the same Ganesha node ID and a distinct Java gateway ID. The shared storage must provide durable writes, coherent reads, atomic rename, and exclusive ownership during host replacement. If using a shared block device, fence the old host before attaching and mounting it on the replacement. If using a shared filesystem, ensure the old Ganesha process is fenced before the replacement starts. Never use object storage for `RecoveryRoot` or allow both gateways to serve the export concurrently.

Pass an operator-owned transition script as `--restart-command`. It must fence the old gateway host, move the stable NFS address, make `RecoveryRoot` available to the replacement, start the Java gateway with a new gateway ID, wait for its OM export lease, then start Ganesha with the original node ID and server identity. Keep the script idempotent and make it return nonzero if fencing, storage activation, or replacement startup fails. The mounted runner keeps a file open across this command, writes and fsyncs data before and after the transition, and compares final bytes. Invoke it with an outer timeout because a hard NFS mount can block during failover:

```sh
timeout -k 5 300 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_reclaim.py \
  --mount /mnt/ozone \
  --restart-command /path/to/fenced-active-passive-transition
```

Accept only when evidence confirms the two daemons ran on different physical hosts, the old host was fenced before replacement service began, both hosts used the same durable recovery path and NFS identity, and the open file was reclaimed without remounting the client. Verify read/append/fsync and final bytes through the reclaim runner, then independently compare the corresponding OzoneFS key. Repeat with a hard power-off of the active host and again in the reverse direction. The runner verifies NFS open-state recovery and NFS-visible data bytes; the transition script and operator logs must establish host identity, fencing, storage semantics, and OM lease takeover. This is a manual active/passive acceptance test, not automatic failover.

```sh
ozone nfs
```

Use `ozone --daemon start nfs` for background operation. The Ganesha FSAL connects to the loopback backend over mutual TLS. The certificate allowlist currently grants each configured Ganesha certificate access to all configured exports; do not use one certificate where separate per-export access is required.

## Native handle contract

From the repository root, with CMake and a C99 compiler installed:

```sh
cmake -S hadoop-ozone/nfs-gateway -B /tmp/ozone-nfs-native -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/ozone-nfs-native
ctest --test-dir /tmp/ozone-nfs-native --output-on-failure
```

Java and C share golden payloads in `hadoop-ozone/common/src/test/resources/nfs/file-handles.txt`. The native test also checks every header byte value and truncated payload length.

## Mounted-file contract

Build the patched upstream Ganesha V5.7 runtime and FSAL_OZONE for Linux from the pinned commit:

```sh
docker build -f hadoop-ozone/nfs-gateway/src/main/ganesha/Dockerfile \
  -t ozone-nfs-ganesha:local hadoop-ozone
docker run --rm ozone-nfs-ganesha:local ganesha.nfsd -v
```

This multi-stage image builds pinned Ganesha V5.7 and FSAL_OZONE, then keeps only the daemon, FSAL, and runtime libraries. It copies the example configuration to `/etc/ganesha/ganesha.conf.example`; mount an operator-owned configuration at `/etc/ganesha/ganesha.conf` and the referenced TLS files under `/etc/ozone`. Persist `/var/lib/nfs/ganesha` across process restarts. On Linux, use host networking when the configured Java gateway endpoint is `127.0.0.1:9878`; this image is an external runtime prototype and is not bundled into the Ozone distribution. NFS-Ganesha is LGPLv3+; the ASF licensing review of the FSAL patches and runtime packaging is still open. Do not publish this combined image as an ASF artifact before that review is complete. See the [OEP licensing and release boundary](../../hadoop-hdds/docs/content/design/nfs-ganesha.md#licensing-and-release-boundary).

For a Linux host with the Java gateway already running on loopback:

```sh
docker run --rm --network host \
  --mount type=bind,src=/etc/ganesha/ganesha.conf,dst=/etc/ganesha/ganesha.conf,readonly \
  --mount type=bind,src=/etc/ozone,dst=/etc/ozone,readonly \
  --mount type=bind,src=/var/lib/ganesha/ozone,dst=/var/lib/nfs/ganesha \
  ozone-nfs-ganesha:local ganesha.nfsd -I 0 -F -L STDOUT -f /etc/ganesha/ganesha.conf
```

On Linux, use Python 3 against an existing NFSv4.1 mount:

```sh
timeout -k 5 300 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_file_transfer.py --mount /mnt/ozone
timeout -k 5 60 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_readonly.py --mount /mnt/ozone
timeout -k 5 60 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_write.py --mount /mnt/ozone
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_reclaim.py \
  --mount /mnt/ozone --restart-command 'sudo systemctl restart nfs-ganesha'
# Run only in a disposable Linux client; kinit replaces its default ticket cache.
kinit -kt /etc/security/keytabs/testuser.keytab -l 1m testuser/om@EXAMPLE.COM
mount -t nfs4 -o vers=4.1,sec=krb5p nfs-gateway.example.com:/ozone /mnt/ozone
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_credential_expiry.py \
  --mount /mnt/ozone --ticket-cache FILE:/tmp/krb5cc_0
timeout -k 5 120 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_ozone.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --read-key ec-file --read-key encrypted-file
# Optional: provide a second NFSv4.1 sec=krb5p mount and its distinct FSO bucket.
timeout -k 5 180 python3 hadoop-ozone/nfs-gateway/src/test/nfs/test_ganesha_ozone.py \
  --mount /mnt/ozone --ozone-uri ofs://om-host/volume/bucket \
  --isolated-mount /mnt/ozone-isolated \
  --isolated-ozone-uri ofs://om-host/volume/other-bucket
timeout -k 5 900 python3 hadoop-ozone/nfs-gateway/src/test/nfs/benchmark_ganesha_ozone.py \
  --mount /mnt/ozone
```

For server-side resident-memory measurements, repeat `--process name=pid` for the local Ganesha, NFS gateway, and OM PIDs. The benchmark samples `/proc/<pid>/status` every 250 ms during both workloads and includes each process's peak RSS and sample count in its JSON output. Run it where those PIDs are visible; a Ganesha container needs host PID visibility or the container's PID namespace.

The runners reject local paths and other NFS versions using `/proc/mounts`. The file-transfer runner creates a temporary directory inside the mount and removes only its test contents. Run it on an export where the test identity may create, read, rename, and remove files and directories. The read-only Ganesha runner requires an NFSv4.1 `sec=krb5p` mount and the Java contract fixture's `child` file; it verifies listing, a ranged read, EOF, and that opening the file for writing is denied. The writable Ganesha runner uses the Java mock fixture's RW export and checks create, staged file size, write, fsync/COMMIT, exact byte readback, rename with stable inode identity, removal, and directory create/remove over the mount. It exercises mock publication only, not Ozone data persistence or crash durability. The reclaim runner requires a command that restarts or replaces Ganesha, keeps a file open across the change, then verifies read, write, and fsync through the reclaimed open state. Persist the configured `RecoveryRoot` and keep the export identity and NFS address unchanged; a replacement must use the saved recovery data before accepting clients. Run only against a test export because the command restarts the server. The credential-expiry runner requires a disposable client with a short-lived TGT and NFS service ticket; it verifies an operation works before expiry and is rejected after the service ticket expires. The Ozone publication runner targets a real FSO bucket mounted through Ganesha and checks out-of-order and overlapping writes, fsync, byte-exact OzoneFS readback, rename with stable inode identity, and removal through both interfaces. Its stale-handle case requires a client mount with `actimeo=0`; it permanently deletes and recreates a test key through Ozone, verifies the new path has a new file ID, and expects the old open handle to return `ESTALE`. Pass both `--isolated-mount` and `--isolated-ozone-uri` to compare same-name, different-content files through a second export and confirm that each mount and OzoneFS bucket sees only its own key. Both mounts must be NFSv4.1 `sec=krb5p`, and the Ozone CLI must be configured to access both buckets. The outer timeout bounds a stalled hard NFS mount.

Coverage includes fresh-file upload and fsync, ranged reads and EOF, empty files, directory listing, file and parent-directory rename with stable inode identity, removal, nonempty-directory errors, and UTF-8 names. Successful fsync here checks the syscall result; host-loss durability requires separate crash tests. Listing all entries does not prove mutation-safe directory cookies.

The benchmark emits JSON with sequential write-plus-fsync throughput, read throughput and checksum, small-file create/write/fsync/delete throughput and latency percentiles, mount options, and client process peak RSS. Optional `--process` arguments add sampled peak RSS for Ganesha, the gateway, and OM. OM request amplification still requires separate collection. The seven file-transfer tests pass against upstream Ganesha V5.7 MEM on Linux/aarch64 with AUTH_SYS. Mounted read-only and writable contracts pass against the Java fixture over FSAL_OZONE with RPCSEC_GSS/Kerberos privacy and mutual TLS. The writable contract verifies staged size before COMMIT, exact mock byte readback afterward, rename with stable inode identity, removal, and directory operations. The Ozone publication runner passes against the secure test cluster and verifies OzoneFS byte-exact readback after mounted writes. On 2026-10-14, the six-test secure mounted contract passed against two NFSv4.1 `sec=krb5p` exports: same-name files with different contents remained isolated in both mounts and their distinct FSO buckets, with different file IDs and byte-exact OzoneFS readback. The FSO mini-cluster contracts verify that Ozone key overwrite/rename, OzoneFS ancestor rename/delete, and S3 Gateway overwrite/delete/multipart part upload are blocked while an NFS file lease is active; rejected overwrites preserve the published bytes, and S3 returns `ConditionalRequestConflict` (409). On 2026-10-14, the standalone reclaim runner passed against a secure Linux NFSv4.1 `sec=krb5p` mount across replacement Ganesha containers with different hostnames and gateway IDs: it kept a file open, reclaimed state using a shared `RecoveryRoot` on a host bind mount, then verified read/write/fsync plus byte-exact final contents. This validates container replacement on one Docker host; separate-host shared-storage recovery and persistent stale handles remain separate acceptance requirements in the OEP.

On 2026-10-13, the credential-expiry runner passed on a disposable Linux client. It created and removed a directory while the Kerberos ticket was valid, waited past the NFS service-ticket expiry, then confirmed a new `mkdir` failed with `EKEYEXPIRED`.

## C++/Java mutual TLS contract

The user approved the optional native gRPC C++/protobuf dependencies on 2026-10-08. The Java module builds with Maven and reuses Ozone's existing gRPC/protobuf dependencies:

```sh
mvn -pl :ozone-nfs-gateway -am install -DskipTests -DskipShade -DskipRecon -DskipDocs
mvn -pl :ozone-nfs-gateway test dependency:copy-dependencies -DincludeScope=test -DskipShade -DskipRecon -DskipDocs
```

Build the Linux transport toolchain and run the contract using the repository mounted at `/workspace`:

```sh
docker build -t ozone-nfs-grpc:local -f hadoop-ozone/nfs-gateway/src/test/native/Dockerfile.grpc hadoop-ozone/nfs-gateway
docker run --rm -v "$PWD:/workspace:ro" ozone-nfs-grpc:local sh -c '
  cmake -S /workspace/hadoop-ozone/nfs-gateway -B /tmp/nfs-build -DOZONE_NFS_GRPC=ON -DCMAKE_BUILD_TYPE=Debug &&
  cmake --build /tmp/nfs-build -j2 &&
  ctest --test-dir /tmp/nfs-build --output-on-failure &&
  python3 /workspace/hadoop-ozone/nfs-gateway/src/test/native/test_grpc_tls.py     --java-classpath "/workspace/hadoop-ozone/nfs-gateway/target/classes:/workspace/hadoop-ozone/nfs-gateway/target/test-classes:/workspace/hadoop-ozone/interface-client/target/classes:/workspace/hadoop-ozone/common/target/classes:/workspace/hadoop-ozone/nfs-gateway/target/dependency/*"     --native-client /tmp/nfs-build/test_ozone_grpc_tls -v'
```

Use your configured Docker context. The image records native package versions in `/opt/ozone-nfs-native-versions.txt`. The verified Linux/aarch64 prototype uses gRPC C++ 1.51.1-4.1build5 and protobuf/compiler 3.21.12-8.2ubuntu0.3; the Java side uses the repository's gRPC 1.77.1 and protobuf 3.25.9. Native generated sources use the same installed compiler/runtime. This is prototype interoperation evidence, not a supported production dependency baseline.

The 21 live tests cover CREATE, READ, WRITE, COMMIT/final close, GETATTR, LOOKUP, and READDIR interoperation over mutual TLS, including UTF-8 lookup, directory continuation and stale verifier handling, export isolation, a CA-trusted but unlisted peer, expired client certificates, backend hostname verification, untrusted server trust, Ganesha export startup, and unsupported service versions. The C adapter also refuses missing TLS material or an unbounded/zero deadline. Six Java tests cover peer/export policy and reject plaintext, non-loopback peers, expired or unlisted certificates, and empty/ambiguous allowlists.

The fixture creates temporary test credentials and exercises GETATTR, LOOKUP, READDIR, and READ plus the backend CREATE, WRITE, COMMIT, and close paths. Capability negotiation includes CREATE when the export lease manager is present. These tests prove wire/TLS behavior, not a KDC login or Ozone data access. Production caller operations still require the authenticated RPCSEC_GSS principal and OM authorization described in the OEP. The mounted secure-cluster contract separately verifies real Ozone reads and write publication through Ganesha and OzoneFS.

## Caller authorization

`NfsCallerAuthorizer` first requires the pinned TLS peer to be authorized for the requested export. It accepts a realm-bearing Kerberos principal with an integrity or privacy protection assertion, applies the configured Hadoop Kerberos name rules, and authorizes the resulting proxy user through an initialized Hadoop `ImpersonationProvider`. The real user must have Kerberos service credentials. Proxy host authorization uses the backend transport peer address; the local Ganesha peer must authenticate the original NFS caller through RPCSEC_GSS before making this assertion.

`NfsGatewayDaemon` logs in with the configured gateway keytab, initializes the operator's impersonation rules, and constructs this authorizer before starting the backend. Each operation uses the returned proxy UGI for its Ozone client; a shared service-user client would lose the caller identity. Secure gateway startup, KDC login, and OM export-lease acquisition are covered by the HA mini-cluster integration test. The gateway periodically checks and renews its keytab TGT; the scheduler lifecycle has a unit test, while live renewal during a mounted workload remains unverified.

Two caller tests cover both protection levels, mapped user and real-user identity, denied users and hosts, missing protection, invalid or unmapped principals, absent TLS context, and service users without Kerberos credentials. Service credentials are mocked: these tests do not prove KDC login or Ganesha's RPCSEC_GSS extraction.

`src/test/nfs/test_ganesha_authorization.py` is a mounted contract for Ozone ACL enforcement. Run it as a non-owner Linux user with a valid Kerberos ticket after allowing that user in both the gateway and OM proxy-user rules:

```sh
python3 src/test/nfs/test_ganesha_authorization.py --mount /mnt/ozone
```

On 2026-10-13, the secure Linux NFSv4.1 `sec=krb5p` mount returned `EACCES` to `testuser2` when OM denied access to the private export root. The normal `testuser` mounted read/write contract still passes with the default proxy-user rule.

`NfsObjectReader` connects an authorized caller to the existing `LookupNfsObject` OM RPC. It rejects mismatched request exports, malformed handles, and cluster/export/incarnation mismatches before opening an OM transport. Transport creation and lookup run under the returned UGI, with no pathname fallback or shared service-user connection; each transport closes on success or failure. Head lookup omits block locations, while data lookup requests the latest authorized locations. `NfsGatewayDaemon` obtains the cluster ID from SCM, validates export bindings, and wires the reader into the production gRPC service. The secure-cluster mounted contracts verify Ozone reads and writes through Ganesha, and a second Kerberos user reaches the OM ACL check and receives `EACCES` when creating in the private export root. That denial test exposed an OM volume/bucket lock-order error; NFS object and child lookups now resolve bucket links and check caller ACLs outside the bucket lock, then revalidate the object path before returning metadata or listing children. Same-gateway NFSv4 restart/reclaim passed on 2026-10-13; mounted host-loss recovery remains unverified.

Four lookup unit tests verify caller identity during transport creation and submission, unchanged identity after the operation, resource cleanup after OM failure, authorization failure before connection, and invalid binding/handle rejection. The FSO identity integration contract exercises this reader through real OM RPC for metadata, block locations, rename continuity, and stale identity after recreation. It also sends capability, GETATTR, LOOKUP, READDIR, and ranged-read calls over the production mutual-TLS gateway server against real mini-cluster data. Its caller authorizer is mocked in a non-secure mini-cluster, so it does not prove Kerberos caller authorization or a mounted Ganesha path to real Ozone data.

## Identity-based ranged reads

`RpcClient.readNfsFile` opens a file by volume/bucket/object identity through OM and uses the same identity for block pipeline refresh. It reuses Ozone's existing input-stream construction, including EC and encryption handling; it never refreshes by the mutable pathname. A client unit test exercises refresh after rename, deletion, and a mismatched replacement response. The secure HA MiniOzoneCluster contract verifies byte-exact EC-backed and KMS-encrypted reads through the production Java gateway over mutual TLS. `test_ganesha_ozone.py --read-key KEY` now compares named files through the mounted FSAL against OzoneFS; mounted EC/KMS execution is still pending.

`NfsObjectReader.read` performs the read under the authorized caller's UGI, closes the client and stream, and limits each request to 1 MiB. Offset is an unsigned 64-bit value: offsets beyond EOF return empty data with EOF, including values above Java's signed range. Zero count before EOF returns empty data without EOF. Premature storage EOF fails rather than reporting successful end of file. Directory reads consult OM before returning the directory error, preserving stale identity and ACL failures.

`NfsGatewayService` exposes CREATE, READ, GETATTR, LOOKUP, and READDIR and enforces TLS/export context before dispatch. OM errors become explicit NFS status responses, malformed handles become BADHANDLE, and storage failures become IO. Hadoop RPC authorization failures map to NFS ACCESS, and the native FSAL translates NFS IO to its protocol status. Failure responses omit data and EOF. Authentication errors retain their gRPC status, while unexpected backend failures expose only INTERNAL. Capability negotiation advertises GETATTR, LOOKUP, READDIR, and READ; it also advertises CREATE when an export lease manager is configured. Mounted writes, Ozone publication, and replacement-container reclaim using persisted Ganesha recovery state are verified; separate-host shared-storage recovery remains a production gate. Root handles are bound to the configured bucket identity and each operation still validates live identity and caller ACLs through OM.

The 30 gateway unit tests cover dispatch, error categories, read bounds, resource cleanup, and identity authorization. The FSO mini-cluster contract also exercises GETATTR, LOOKUP, READDIR, and real datanode reads through the gateway reader and mutual-TLS gRPC server: partial and final ranges, zero-count and unsigned beyond-EOF reads, empty files, renamed files and parents, stale reads after recreation, and mutation-safe directory continuation. These tests do not establish Ganesha callbacks against real Ozone data, mounted NFS, Kerberos end to end, or crash durability.

## Native C++ READ interoperation

The C ABI provides `ozone_grpc_create`, `ozone_grpc_write`, `ozone_grpc_commit`, `ozone_grpc_read`, `ozone_grpc_getattr`, `ozone_grpc_lookup`, and `ozone_grpc_readdir`, with explicit export UUID, caller identity/protection, persistent handles, and bounded outputs. CREATE is a negotiated backend capability when the export lease manager is configured; the FSAL also implements WRITE callbacks. READ and WRITE are limited to 1 MiB and READDIR to 256 entries; calls use a gRPC deadline, clear outputs on failure, and reject malformed responses. Caller principal bytes use an explicit length and are never truncated at NUL.

Twenty-one live Linux C++/Java mTLS tests exercise CREATE, READ data and EOF, WRITE, COMMIT and final close, GETATTR/LOOKUP attributes and errors, READDIR continuation with and without a backend verifier, export isolation, denied callers, certificate policy, Ganesha export startup, and unsupported versions. The fixture invokes the production `NfsGatewayService` dispatcher with a mock reader; it proves wire/TLS behavior, including range data, not a KDC login or Ozone data access. The mini-cluster contract separately covers real Ozone/datanode reads. Ganesha loads the module, initializes an export, and completes its TLS capability handshake against the fixture in a Linux startup smoke test. Mounted contracts verify fixture operations, while secure-cluster publication and replacement-container reclaim verify real Ozone data through Ganesha; separate-host shared-storage recovery remains open.

The live native/Java test service checks caller principal and privacy fields in its test reader. An embedded-NUL principal survives the C ABI and wire, then receives UNAUTHENTICATED; oversized and null principal buffers fail locally. These are identity-transport checks with a mock reader, not a Kerberos/GSS login test.
