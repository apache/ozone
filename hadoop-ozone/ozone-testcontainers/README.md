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

# Ozone Testcontainers module

A Testcontainers module so JVM projects can start a single-node Ozone with S3
Gateway from their integration tests the same way they use the MinIO / LocalStack
modules. Motivated by the container-based test-framework interest (e.g. Trino)
raised on [HDDS-14893](https://issues.apache.org/jira/browse/HDDS-14893).

## Status

**Early / not yet released.** The module is wired into the reactor
(`hadoop-ozone/pom.xml`) and builds clean, but the base image and packaging are
still under discussion (see HDDS-14893). The smoke test requires a running Docker
daemon and is skipped automatically when Docker is unavailable.

## What it does

- `OzoneContainer` wraps the all-in-one quickstart image
  `apache/ozone:2.2.0-all-in-one`
  ([HDDS-14452](https://issues.apache.org/jira/browse/HDDS-14452) /
  `ozone-docker#49`), which runs SCM + OM + datanode + S3 Gateway in one
  container and exposes S3G on 9878.
- Exposes `getS3Endpoint()`, `getAccessKey()`, `getSecretKey()`, `getRegion()`.
- `OzoneContainerSmokeTest` builds an AWS SDK v2 `S3Client`
  (`endpointOverride` + `forcePathStyle` + static credentials) and runs
  create bucket / put / get / list.

## Run

```
mvn -pl :ozone-testcontainers test
```

With Docker running: create/put/get/list all pass and the container is
auto-removed. Without Docker the test is skipped.

## Notes

- Non-secure S3G accepts any access key / secret.
- Readiness is handled by the container: it waits for the S3 Gateway port and
  then runs `ozone admin safemode wait` inside the container, so the cluster is
  ready for S3 operations as soon as `start()` returns.
- `ozone local run` (needs
  [HDDS-15087](https://issues.apache.org/jira/browse/HDDS-15087)) would give a
  cleaner env-driven image later, but is not required for a working module.
