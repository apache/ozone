# AGENTS instructions

## Working Style

- Prefer the smallest correct change. Do not add features, abstractions, refactors, or cleanup that were not asked for.
- Keep diffs surgical. Every changed line should trace back to the task.
  Do not reformat, rewrap, or rename adjacent code "while you are here".
- Match the surrounding module before introducing a new pattern.
  Reuse existing Ozone helpers, test scaffolding, and service abstractions where possible.
- Reuse existing Ozone and Ratis utilities when the surrounding code already uses them.
  Prefer extending an existing helper over duplicating logic or adding a new one-off abstraction.
- If there are multiple reasonable interpretations, state the tradeoff and ask instead of guessing.
- Do not wrap lines early just to make them look uniform. The checkstyle maximum (see `hadoop-hdds/dev-support/checkstyle/checkstyle.xml`) is 120 characters for Java. Use the full 120 characters before wrapping; never break a line that fits on one line.
- Use established Ozone vocabulary in code, docs, and PR text:
  SCM, OM, datanode, container, pipeline, volume, bucket, key, snapshot,
  Recon, FSO, OBS, and S3 Gateway.
  Avoid inventing new architecture terms unless the repo already uses them.

## Repository Snapshot

Apache Ozone is a multi-module Maven project. The root coordinates and version live in [`pom.xml`](./pom.xml).

Tech stack:

- Java 17 by default (`ozone.java.version.server` in `pom.xml`). Modules that set `maven.compiler.release` to
  `${ozone.java.version.client}` in their own `pom.xml` still target Java 8.
- Maven build
- Hadoop RPC and gRPC over Protobuf
- RocksDB for persistent metadata
- Apache Ratis for replicated state
- JUnit 5 for tests

Two top-level aggregators:

- `hadoop-hdds/`: storage layer and shared infrastructure.
  Key submodules include `server-scm`, `container-service`, `framework`,
  `managed-rocksdb`, and `interface-{admin,client,server}`.
- `hadoop-ozone/`: Ozone services and clients.
  Key submodules include `ozone-manager`, `s3gateway`, `recon`, `datanode`,
  `dist`, `integration-test*`, and `ozonefs*`.

Service boundaries:

1. SCM manages containers, pipelines, and replication metadata.
2. OM manages namespace, keys, buckets, volumes, snapshots, and most user-visible metadata.
3. Datanodes serve container data and participate in Ratis pipelines.
4. Recon provides observability and derived metadata views.
5. S3 Gateway and OzoneFS expose external APIs on top of OM and HDDS services.

Cross-cutting changes often span multiple layers.
A feature or bug fix may need updates in `hadoop-hdds/interface-*`,
server-side handling, client translation code, and integration tests.

## Local Environment

- Use a JDK 21 runtime locally. Before using Java 9+ syntax or standard-library APIs, check the module's
  `maven.compiler.release` in its `pom.xml`; Maven `--release` governs both language level and API surface.
- Ozone formatting conventions are shared through `.editorconfig`.
- If Maven behaves unexpectedly, check `java -version` and `mvn -version` first.

## Commands

Default local build flags:

- Use `-DskipShade -DskipRecon` for iterative local work.
- Drop `-DskipShade` only when you need filesystem artifacts or tests that depend on the shaded Ozone FS jar.
- Drop `-DskipRecon` only when you are changing Recon UI or server behavior that must be built locally.

Primary commands:

- Iterative full build: `mvn clean install -DskipTests -DskipShade -DskipRecon`
- Full compile/verify smoke check: `mvn clean verify -DskipTests -DskipShade -DskipRecon`
- Rebuild one module and its dependencies:
  `mvn -pl :ozone-manager -am install -DskipTests -DskipShade -DskipRecon`
- Run one unit test class: `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock -DskipShade -DskipRecon`
- Run one unit test method:
  `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock#testLockingOrder -DskipShade -DskipRecon`
- Run one integration test class:
  `mvn -pl :ozone-integration-test test -Dtest=TestOmContainerLocationCache -DskipShade -DskipRecon`

CI-aligned local checks live under
[`hadoop-ozone/dev-support/checks/`](./hadoop-ozone/dev-support/checks/).
Prefer these when validating a change because they match CI layout and reporting:

- `./hadoop-ozone/dev-support/checks/unit.sh`
- `./hadoop-ozone/dev-support/checks/integration.sh`
- `./hadoop-ozone/dev-support/checks/checkstyle.sh`
- `./hadoop-ozone/dev-support/checks/rat.sh`
- `./hadoop-ozone/dev-support/checks/author.sh`

Notes:

- The check scripts write results under `target/<check-name>/` (or `$OUTPUT_DIR`).
- `build.sh` honors `FAIL_FAST=true`, `ITERATIONS=N`, and `OZONE_WITH_COVERAGE=true`.

### Local Cluster

- Build a runnable distribution when you need compose assets or a local tarball: `mvn -Pdist -DskipTests package`
- Start the default compose cluster from
  `hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone`:
  `OZONE_REPLICATION_FACTOR=3 ./run.sh -d`
- `.run/` contains IntelliJ run configurations for SCM, OM, Recon, datanodes, shells, S3 Gateway, and HA variants.

## Repository Structure

Key paths:

- `hadoop-hdds/interface-*`: Protobuf definitions and protocol-facing interfaces
- `hadoop-hdds/server-scm`: SCM server behavior
- `hadoop-hdds/container-service`: datanode-side container handling
- `hadoop-hdds/framework`: shared service infrastructure
- `hadoop-hdds/managed-rocksdb`: RocksDB wrappers and helpers
- `hadoop-ozone/ozone-manager`: OM request handling and namespace logic
- `hadoop-ozone/s3gateway`: S3-compatible gateway
- `hadoop-ozone/recon`: Recon backend and UI
- `hadoop-ozone/datanode`: Ozone datanode service pieces outside HDDS container-service
- `hadoop-ozone/integration-test*`: Mini-cluster and integration coverage
- `hadoop-ozone/dist`: distribution assembly and compose definitions
- `hadoop-ozone/dev-support/checks`: scripts that mirror CI checks
- `.run/`: IDE launch configurations for local services and HA topologies

## Contribution Guidelines

Follow the guidelines in [`CONTRIBUTING.md`](./CONTRIBUTING.md): the pull request workflow and guidelines,
code conventions, design guidelines, and tests. Committers should also follow its committer guidelines.

Before wrapping up a change:

- For a non-trivial change, run `./hadoop-ozone/dev-support/checks/checkstyle.sh`.
- If you added files or changed license headers, run `./hadoop-ozone/dev-support/checks/rat.sh`.
- If you touched shell tooling, run `./hadoop-ozone/dev-support/checks/bats.sh`.
- Use `acceptance.sh` and `kubernetes.sh` only when the changed area actually depends on those environments.

## Ask First

- Large new features or design changes that may need an Ozone Enhancement Proposal
- Large cross-module refactors that are not required for the task
- New third-party dependencies
- Protobuf or RPC changes with compatibility impact
- RocksDB layout, metadata schema, or upgrade/finalization changes
- Broad terminology or naming cleanups across many files

## Never

- Commit secrets, credentials, or tokens
- Use destructive git commands unless explicitly requested
- Hand-edit generated files when the source or generation workflow exists
- Add unrelated cleanup, formatting churn, or speculative abstractions to the same change

## References

- [`CONTRIBUTING.md`](./CONTRIBUTING.md)
- [`.github/pull_request_template.md`](./.github/pull_request_template.md)
- [`hadoop-ozone/dev-support/checks/README.md`](./hadoop-ozone/dev-support/checks/README.md)
- [`hadoop-hdds/dev-support/checkstyle/checkstyle.xml`](./hadoop-hdds/dev-support/checkstyle/checkstyle.xml)
- [`dev-support/rat/rat-exclusions.txt`](./dev-support/rat/rat-exclusions.txt)
- [Ozone Enhancement Proposals](https://ozone.apache.org/docs/next/developer-guide/project/enhancement-proposal)

## Security

When assessing a potential security vulnerability in Apache Ozone, complete
these steps before drafting any report or reaching any security conclusion.

### Step 1 — Read the threat model
Read **[THREAT_MODEL.md](THREAT_MODEL.md)**: the multi-service trust boundaries,
the **secure mode** knob, the properties provided vs. left to the operator, and
the known non-findings.

### Step 2 — Read the security policy
Read **[SECURITY.md](SECURITY.md)** for how to report.

### Key scoping facts (see THREAT_MODEL.md)
- Ozone is a cluster of network services (S3 Gateway, OM, SCM/internal-CA,
  Datanodes/Ratis, Recon). Roles: untrusted client, authenticated-but-
  unauthorized user, operator, service peer, bounded-Byzantine datanode.
- **Secure mode** (`ozone.security.enabled=true`) is load-bearing: a finding
  that only manifests in non-secure (dev) mode is out of model (section 5a).
- Ozone does **not** own its dependencies' security — the Kerberos KDC, Ranger
  policy correctness, the SCM CA private key, KMS keys, and network isolation
  are the operator's (sections 3/9/10). Route such findings there.
- Ratis (Raft) safety holds under an honest majority; a Byzantine majority is
  out of scope.
- integration-test modules, and test utilities are out of scope.

### Then assess
Route the finding to exactly one disposition in **THREAT_MODEL.md section 13**,
citing the section. If it cannot be routed, it is a `MODEL-GAP` — surface it.
