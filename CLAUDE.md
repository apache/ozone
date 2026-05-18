# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

Apache Ozone — a scalable, distributed object/file store. This is a multi-module Maven project (`org.apache.ozone:ozone-main`, version `2.2.0-SNAPSHOT`). Two top-level Maven aggregators:

- `hadoop-hdds/` — Hadoop Distributed Data Store: the storage layer (SCM, datanode container service, RocksDB integration, common protocols, crypto, framework). Submodules include `server-scm`, `container-service`, `framework`, `managed-rocksdb`, `rocksdb-checkpoint-differ`, `interface-{admin,client,server}`, `common`, `client`, `config`, etc.
- `hadoop-ozone/` — Ozone services and clients on top of HDDS. Key submodules: `ozone-manager` (OM), `s3gateway`, `recon` (UI/monitoring), `datanode`, `client`, `csi` (CSI driver), `ozonefs*` (Hadoop-compatible FS adapters, including a `ozonefs-shaded` jar), `freon` (load generator), `dist` (assembles the binary distribution and Docker compose configs), `integration-test*` (Java mini-cluster tests), `mini-cluster`, `tools`, `cli-{admin,debug,repair,shell}`.

The big-picture component split: SCM manages containers/pipelines, OM manages namespace/keys, datanodes serve container data via Ratis (RAFT), Recon provides observability, and S3 Gateway / OzoneFS expose S3 and Hadoop FS APIs. Module boundaries follow this split — when changing a feature, expect edits across `hadoop-hdds/interface-*` (proto), an HDDS server module, and one of `ozone-manager` / `s3gateway` / `recon`. Integration tests for these flows live exclusively in `hadoop-ozone/integration-test*`.

## Build

```bash
mvn clean verify -DskipTests       # full build
mvn clean install -DskipTests      # build + install to local repo (needed when iterating across modules)
```

Useful flags (composable):
- `-DskipShade` — skip the shaded Ozone FS jar (saves time; needed for filesystem integration tests if absent)
- `-DskipRecon` — skip Recon Web UI (~2 min faster)
- `-DskipDocs` — skip Hugo docs build
- `-Pdist` — build the binary tarball under `hadoop-ozone/dist/target/`

Single-module rebuild after a change (note the leading colon for the artifactId):
```bash
mvn -pl :ozone-manager -am install -DskipTests
```

## Running tests

`hadoop-ozone/dev-support/checks/` contains the same scripts CI runs. Each writes results to `target/<check-name>/` (or `$OUTPUT_DIR`) and exits non-zero on failure. Most useful locally:

- `unit.sh` — pure unit tests (excludes the `ozone-integration-test*` and `mini-chaos-tests` modules)
- `integration.sh` — Java mini-cluster integration tests (slow)
- `checkstyle.sh`, `pmd.sh`, `findbugs.sh`, `rat.sh`, `author.sh` — static checks
- `acceptance.sh` — robot-framework smoketests against a real docker-compose cluster (requires `docker`, `docker-compose`, `jq`)
- `kubernetes.sh` — limited k8s test suite

Direct Maven equivalents (for running a single test):
```bash
# Single test class
mvn -pl :ozone-manager test -Dtest=TestOzoneManager
# Single test method
mvn -pl :ozone-manager test -Dtest=TestOzoneManager#testSomething
# Integration tests live in their own modules and require -DskipShade etc.
mvn -pl :ozone-integration-test test -Dtest=TestOmContainerLocationCache -DskipShade -DskipRecon
```

Re-run flaky tests with the `test-flaky` profile (CI does this): `integration.sh -Ptest-flaky` adds `-Dsurefire.rerunFailingTestsCount=5`.

`build.sh` honors `FAIL_FAST=true`, `ITERATIONS=N`, and `OZONE_WITH_COVERAGE=true` env vars; pass extra Maven args after the script name.

## Running a local cluster

After a `-Pdist` build:
```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
OZONE_REPLICATION_FACTOR=3 ./run.sh -d
```
`hadoop-ozone/dist/src/main/compose/` has many topologies (`ozone-ha`, `ozonesecure`, `upgrade`, `xcompat`, etc.). For IDE-launched components there are pre-baked IntelliJ run configs in `.run/`; the documented startup order is SCM init → SCM → OM init → OM → Recon → datanodes.

## Code conventions

- 2-space indentation, 120-char line limit, Apache license header required (enforced by Checkstyle and RAT).
- No `@author` tags (enforced by `author.sh`).
- Checkstyle config: `hadoop-hdds/dev-support/checkstyle/checkstyle.xml`.
- For non-trivial features, follow the OEP (Ozone Enhancement Proposal) process before coding — design docs are required in Markdown.

## Jira / PR workflow

- Every change needs an `HDDS-NNNN` Jira; PR titles start with the Jira id (see recent commits like `HDDS-15277. Refactor ...`).
- Update branches via `git merge --no-edit origin/master`, **not** rebase. Avoid force-push on PRs.
- The `build-branch` GitHub Actions workflow must be enabled in the contributor's fork before opening a PR.
- Before wrapping up a change (e.g. before posting a PR), run `./hadoop-ozone/dev-support/checks/checkstyle.sh` locally to catch violations — otherwise the checkstyle CI job will fail immediately on the PR.
