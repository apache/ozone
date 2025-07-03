---
title: Mount Ozone as a Local File System
menu:
  main:
    parent: "Client Interfaces"

summary: Ozone can be mounted as a local file system using S3 Gateway and goofys.
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

## Overview

This document explains how to mount an Ozone bucket to a POSIX-compatible local file system using the S3 Gateway and `goofys`, a third-party S3 compatible FUSE (Filesystem in Userspace) implementation. This allows you to interact with your Ozone bucket as if it were a local directory.

## Performance Warning

**Important:** While convenient, mounting Ozone via `goofys` and S3 Gateway will likely result in significantly lower performance compared to using native Ozone file system interfaces (e.g., Ozone File System (OFS) or Ozone Distributed File System (O3FS)). This method is generally suitable for light workloads, occasional access, or scenarios where POSIX compatibility is a strict requirement and performance is not critical. For high-performance applications, consider using Ozone's native client interfaces.

## Prerequisites

*   An running Ozone cluster with S3 Gateway enabled.
*   `goofys` installed on your local machine. Refer to the [goofys project page](https://github.com/kahing/goofys) for installation instructions.

## Command Line Usage

To mount an Ozone bucket, you will need the S3 access key and secret key for an Ozone user, and the S3 Gateway endpoint.

1.  **Export S3 credentials:**

    ```bash
    export AWS_ACCESS_KEY_ID=<your_s3_access_key>
    export AWS_SECRET_ACCESS_KEY=<your_s3_secret_key>
    ```

2.  **Create a mount point:**

    ```bash
    mkdir /mnt/ozone_bucket
    ```

3.  **Mount the Ozone bucket using `goofys`:**

    ```bash
    goofys --endpoint <s3_gateway_endpoint> <bucket_name> /mnt/ozone_bucket
    ```

    Replace `<s3_gateway_endpoint>` with the actual endpoint of your Ozone S3 Gateway (e.g., `http://localhost:9878`), and `<bucket_name>` with the name of the Ozone bucket you wish to mount.

    Example:

    ```bash
    goofys --endpoint http://localhost:9878 mybucket /mnt/ozone_bucket
    ```

4.  **Verify the mount:**

    You can now navigate into `/mnt/ozone_bucket` and perform file operations as you would on a local file system.

    ```bash
    ls /mnt/ozone_bucket
    echo "Hello Ozone" > /mnt/ozone_bucket/hello.txt
    cat /mnt/ozone_bucket/hello.txt
    ```

5.  **Unmount the bucket:**

    To unmount the bucket when you are finished:

    ```bash
    fusermount -u /mnt/ozone_bucket
    ```

## Related Links

*   [Ozone S3 Protocol Documentation]({{< ref "interface/S3.md" >}})
*   [goofys project page](https://github.com/kahing/goofys)
