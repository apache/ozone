---
title: "Testcontainers"
summary: Start a single-node Ozone with S3 Gateway from JVM integration tests using Testcontainers.
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

The `ozone-testcontainers` module lets JVM integration tests start a single-node
Ozone with an S3 Gateway inside a Docker container, the same way tests use the
MinIO or LocalStack [Testcontainers](https://testcontainers.com/) modules. It
wraps the all-in-one Ozone image, which runs SCM, OM, a datanode and the S3
Gateway in one container.

Requires a running Docker daemon. The smoke test is skipped automatically when
Docker is not available.

## Dependency

```xml
<dependency>
  <groupId>org.apache.ozone</groupId>
  <artifactId>ozone-testcontainers</artifactId>
  <scope>test</scope>
</dependency>
```

## Usage

```java
try (OzoneContainer ozone = new OzoneContainer()) {
  ozone.start();

  S3Client s3 = S3Client.builder()
      .endpointOverride(URI.create(ozone.getS3Endpoint()))
      .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ozone.getAccessKey(), ozone.getSecretKey())))
      .region(Region.of(ozone.getRegion()))
      .forcePathStyle(true)
      .build();

  s3.createBucket(b -> b.bucket("my-bucket"));
  // ... put / get / list objects
}
```

In non-secure mode the S3 Gateway accepts any access key and secret, so the
accessors simply provide a consistent set of credentials to sign requests with.
The container waits for the S3 Gateway port and for SCM to leave safe mode, so
the cluster is ready for S3 operations as soon as `start()` returns.

## Migrating from MinIO

The container exposes the same shape as the MinIO Testcontainers module, so a
test using `MinIOContainer` can switch with small changes:

| MinIO | Ozone |
|-------|-------|
| `new MinIOContainer(...)` | `new OzoneContainer()` |
| `getS3URL()` | `getS3Endpoint()` |
| `getUserName()` | `getAccessKey()` |
| `getPassword()` | `getSecretKey()` |
| n/a | `getRegion()` |
