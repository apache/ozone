# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

Apache Ozone — a scalable, distributed object/file store. This is a multi-module Maven project; root coordinates and current version are in [`pom.xml`](./pom.xml) (`groupId` / `artifactId` / `version`). Two top-level Maven aggregators:

- `hadoop-hdds/` — Hadoop Distributed Data Store: the storage layer (SCM, datanode container service, RocksDB integration, common protocols, crypto, framework). Load-bearing submodules: `server-scm`, `container-service`, `framework`, `managed-rocksdb`, `interface-{admin,client,server}`. See `hadoop-hdds/pom.xml` for the full list.
- `hadoop-ozone/` — Ozone services and clients on top of HDDS. Load-bearing submodules: `ozone-manager` (OM), `s3gateway`, `recon` (UI/monitoring), `datanode`, `dist` (binary tarball + Docker compose configs), `integration-test*` (Java mini-cluster tests), plus `ozonefs*` (Hadoop-compatible FS adapters). See `hadoop-ozone/pom.xml` for the full list.

The big-picture component split: SCM manages containers/pipelines, OM manages namespace/keys, datanodes serve container data via Ratis (RAFT), Recon provides observability, and S3 Gateway / OzoneFS expose S3 and Hadoop FS APIs. Module boundaries follow this split — when changing a feature, expect edits across `hadoop-hdds/interface-*` (proto), an HDDS server module, and one of `ozone-manager` / `s3gateway` / `recon`. Integration tests for these flows live exclusively in `hadoop-ozone/integration-test*`.

## Build

```bash
mvn clean install -DskipTests      # build + install to local repo (use this when iterating across modules; required before single-module rebuilds resolve correctly)
mvn clean verify -DskipTests       # end-to-end compile/verify without installing — useful as a smoke check, not for iterative dev
```

Useful flags (composable):
- `-DskipShade` — skip building the shaded Ozone FS jar (saves time; omit this flag if you need to run filesystem integration tests, which require the shaded jar)
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
mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock
# Single test method
mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock#testSomething
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

## Conventions and PR workflow

The authoritative source is [`CONTRIBUTING.md`](./CONTRIBUTING.md). Highlights worth surfacing here:

- PR titles start with the Jira id (e.g. `HDDS-12345. Short description`); every change needs an `HDDS-NNNN` Jira.
- Update branches via `git merge --no-edit origin/master`, **not** rebase; avoid force-push.
- 2-space indent, 120-char lines, Apache license header on most files (enforced by Checkstyle and RAT — Markdown excludes are managed in `dev-support/rat/rat-exclusions.txt`); no `@author` tags. Checkstyle config: `hadoop-hdds/dev-support/checkstyle/checkstyle.xml`.
- For non-trivial features, follow the OEP (Ozone Enhancement Proposal) process — design docs in Markdown.
- Before wrapping up a change (e.g. before posting a PR), run `./hadoop-ozone/dev-support/checks/checkstyle.sh` locally to catch violations — otherwise the checkstyle CI job will fail immediately on the PR. `rat.sh` is also worth running if you've added new file types.
