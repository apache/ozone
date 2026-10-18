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

**Incubating, not yet released.** The API may change in a future release.

## Usage

Add `org.apache.ozone:ozone-testcontainers` as a test dependency, then:

```java
try (OzoneContainer ozone = new OzoneContainer()) {
  ozone.start();
  try (S3Client s3 = S3Client.builder()
      .endpointOverride(URI.create(ozone.getS3Endpoint()))
      .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ozone.getAccessKey(), ozone.getSecretKey())))
      .region(Region.of(ozone.getRegion()))
      .forcePathStyle(true)
      .build()) {
    // ...
  }
}
```

The default image is the all-in-one quickstart image
([HDDS-14452](https://issues.apache.org/jira/browse/HDDS-14452)); pass a different one with
`new OzoneContainer(DockerImageName)`. See the `OzoneContainer` javadoc for credentials and
readiness behavior.

## Run

```
mvn -pl :ozone-testcontainers test
```

The S3 smoke test needs a running Docker daemon and is skipped without one; the readiness
unit tests always run.

## Notes

- An image based on `ozone local run` (available since
  [HDDS-15087](https://issues.apache.org/jira/browse/HDDS-15087)) could replace the
  all-in-one image in a follow-up.
