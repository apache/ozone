# Ozone build, test, and check commands

Reference for building, testing, running, and checking Apache Ozone locally. Read on demand;
this file is not loaded into agent context at startup.

## Default local build flags

- Use `-DskipShade -DskipRecon -DskipDocs` for iterative local work.
- Drop `-DskipShade` only when you need filesystem artifacts or tests that depend on the
  shaded Ozone FS jar.
- Drop `-DskipRecon` only when changing Recon UI or server behavior that must be built locally.
- Drop `-DskipDocs` only when changing docs or doc-generation logic.

## Primary commands

- Iterative full build: `mvn clean install -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Full compile/verify smoke check:
  `mvn clean verify -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Rebuild one module and its dependencies:
  `mvn -pl :ozone-manager -am install -DskipTests -DskipShade -DskipRecon -DskipDocs`
- Run one unit test class:
  `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock -DskipShade -DskipRecon -DskipDocs`
- Run one unit test method:
  `mvn -pl :ozone-manager test -Dtest=TestOzoneManagerLock#testLockingOrder -DskipShade -DskipRecon -DskipDocs`
- Run one integration test class:
  `mvn -pl :ozone-integration-test test -Dtest=TestOmContainerLocationCache -DskipShade -DskipRecon`

## CI-aligned checks

Scripts live under `hadoop-ozone/dev-support/checks/`. Prefer them for validation — they
match CI layout and reporting. Grouped by runtime so you can prefer cheap checks and avoid
surprise long runs:

- quick (< 2 min): `author.sh`, `bats.sh`, `rat.sh`, `docs.sh`, `dependency.sh`,
  `checkstyle.sh`, `pmd.sh`
- moderate (~10 min): `findbugs.sh` (SpotBugs), `kubernetes.sh`
- slow (~1 hr+): `unit.sh`, `integration.sh`, `acceptance.sh`

Notes:

- `build.sh` compiles Ozone; it honors `FAIL_FAST=true`, `ITERATIONS=N`, and
  `OZONE_WITH_COVERAGE=true`.
- Most checks write results under `target/<check-name>/` (or `$OUTPUT_DIR`).
- `integration` and `acceptance` accept arguments to limit the set of tests run.
- Use `acceptance.sh` and `kubernetes.sh` only when the changed area depends on those
  environments.

## Local cluster

- Build a runnable distribution when you need compose assets or a local tarball:
  `mvn -Pdist -DskipTests package`
- Start the default compose cluster from
  `hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone`:
  `OZONE_REPLICATION_FACTOR=3 ./run.sh -d`
- `.run/` contains IntelliJ run configurations for SCM, OM, Recon, datanodes, shells, S3
  Gateway, and HA variants. Start order: SCM init → SCM → OM init → OM → Recon → datanodes.
