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
- Do not wrap lines early just to make them look uniform. The project limit is 120 characters; use the space.
- Use established Ozone vocabulary in code, docs, and PR text:
  SCM, OM, datanode, container, pipeline, volume, bucket, key, snapshot,
  Recon, FSO, OBS, and S3 Gateway.
  Avoid inventing new architecture terms unless the repo already uses them.

## Repository Snapshot

Apache Ozone is a multi-module Maven project. The root coordinates and version live in [`pom.xml`](./pom.xml).

Tech stack:

- Java 8 bytecode with JDK 21 runtime compatibility (see the `[21,]` profile in `pom.xml`)
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

- Use a JDK 21 runtime locally. Source and target compatibility remain Java 8.
- Ozone formatting conventions are shared through `.editorconfig`.
- If Maven behaves unexpectedly, check `java -version` and `mvn -version` first.

## Commands

Default local build flags:

- Use `-DskipShade -DskipRecon -DskipDocs` for iterative local work.
- Drop `-DskipShade` only when you need filesystem artifacts or tests that depend on the shaded Ozone FS jar.
- Drop `-DskipRecon` only when you are changing Recon UI or server behavior that must be built locally.
- Drop `-DskipDocs` only when you are changing docs or doc-generation logic.

Primary commands:

- Iterative full build: `mvn clean install -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Full compile/verify smoke check: `mvn clean verify -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Rebuild one module and its dependencies:
  `mvn -pl :ozone-manager -am install -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Run one unit test class: `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock -DskipShade -DskipRecon -DskipDocs`
- Run one unit test method:
  `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock#testLockingOrder -DskipShade -DskipRecon -DskipDocs`
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

## Change Boundaries

- Keep service responsibilities separated.
  Do not move OM logic into SCM paths, bypass existing request/response layers,
  or introduce cross-service shortcuts just because they are convenient.
- When changing a wire type, expect to update the Protobuf definition,
  translators, server-side logic, and relevant compatibility or integration tests.
- Prefer existing bucket-layout, snapshot, and upgrade abstractions over one-off conditionals.
- Do not hand-edit generated sources or generated web artifacts when a source file or generation step exists.
- For integration coverage, extend an existing suite, base class, or cluster provider
  before creating a new `MiniOzoneCluster` lifecycle.
  Reuse existing cluster utilities where practical.

## Coding Standards

- Use 2-space indentation and stay within 120 characters.
- Add the Apache license header to new files unless the surrounding area is explicitly exempted by RAT configuration.
- Do not add `@author` tags.
- Keep comments concrete and local to the code. Avoid vague architecture prose or newly invented terminology.
- Prefer existing helpers and utility methods over new abstractions for single-call-site use.
- When touching code that already follows a specific local pattern,
  stay consistent with that pattern instead of normalizing the whole file.

## Testing Standards

- New behavior and bug fixes should come with tests.
- Start with the narrowest useful test:
  - unit tests for local logic
  - integration tests when the behavior depends on service boundaries, cluster lifecycle, storage, RPC, or upgrade flows
- When adding integration coverage, prefer merging it into an existing suite
  over creating a brand-new test class that spins up another cluster for similar coverage.
- Before wrapping up a non-trivial change, run `./hadoop-ozone/dev-support/checks/checkstyle.sh`.
- If you added files or changed license headers, run `./hadoop-ozone/dev-support/checks/rat.sh`.
- If you touched shell tooling, run `./hadoop-ozone/dev-support/checks/bats.sh`.
- Use `acceptance.sh` and `kubernetes.sh` only when the changed area actually depends on those environments.

## Commits and PRs

- Every change should map to an Apache Jira in the HDDS project.
- Branch names usually start with the Jira ID, for example `HDDS-1234`.
- PR titles must be `HDDS-1234. Short summary of the change`.
- Prefer commit subjects that also start with the Jira ID when it is known,
  for example `HDDS-1234. Fix snapshot purge regression`.
- For larger changes, use incremental commits so reviewers can inspect the delta.
  Do not rewrite branch history unless explicitly asked.
- To bring a branch up to date with `master`, merge instead of rebasing: `git merge --no-edit origin/master`
- Avoid force-push when updating a PR unless a maintainer explicitly asks for rewritten history.
- PR descriptions should include the Jira link, the problem statement,
  the chosen approach, and how the patch was tested.
- When non-trivial content is generated with AI tooling,
  disclose it in the PR description as `Generated-by: TOOL (MODEL)`.
  See the ASF generative tooling policy.

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
- [Ozone Enhancement Proposals](https://ozone.apache.org/docs/edge/design/ozone-enhancement-proposals.html)

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
