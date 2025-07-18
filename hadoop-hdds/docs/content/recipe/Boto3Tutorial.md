---
title: Access Ozone using Boto3 (Docker Quickstart)
linkTitle: Boto3 Access (Docker)
description: Step-by-step tutorial for accessing Ozone from Python using Boto3 and the S3 Gateway in a Docker environment.
weight: 12
---

<!--
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

This tutorial demonstrates how to access Apache Ozone from Python using **Boto3**, via Ozone's S3 Gateway, with Ozone running in Docker.

## Prerequisites

- Docker and Docker Compose installed.
- Python 3.x environment.

## Steps

### 1️⃣ Start Ozone in Docker

Download the latest Docker Compose file for Ozone and start the cluster with 3 DataNodes:

```bash
curl -O https://raw.githubusercontent.com/apache/ozone-docker/refs/heads/latest/docker-compose.yaml
docker compose up -d --scale datanode=3
```

### 2️⃣ Connect to the SCM Container

```bash
docker exec -it <your-scm-container-name-or-id> bash
```
> Change the container id `<your-scm-container-name-or-id>` to your actual container id.

The rest of the tutorial will run on this container.

Create a **bucket** inside the volume **s3v**:

```bash
ozone sh bucket create s3v/bucket
```

### 3️⃣ Install Boto3 in Your Python Environment

```bash
pip install boto3
```

### 4️⃣ Access Ozone via Boto3 and the S3 Gateway

Create a Python script (`ozone_boto3_example.py`) with the following content:

```python
#!/usr/bin/python
import boto3

# Create a local file to upload
with open("localfile.txt", "w") as f:
    f.write("Hello from Ozone via Boto3!\n")

# Configure Boto3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://s3g:9878',
    aws_access_key_id='ozone-access-key',
    aws_secret_access_key='ozone-secret-key'
)

# List buckets
response = s3.list_buckets()
print("Buckets:", response['Buckets'])

# Upload the file
s3.upload_file('localfile.txt', 'bucket', 'file.txt')
print("Uploaded 'localfile.txt' to 'bucket/file.txt'")

# Download the file back
s3.download_file('bucket', 'file.txt', 'downloaded.txt')
print("Downloaded 'file.txt' as 'downloaded.txt'")
```

Run the script:

```bash
python ozone_boto3_example.py
```

✅ You have now accessed Ozone from Python using Boto3 and verified both upload and download operations.

## Notes

- The S3 Gateway listens on port `9878` by default.
- The `Bucket` parameter in Boto3 calls (e.g., `'bucket'` in the script) should be the name of the Ozone bucket created under the `s3v` volume (i.e., the `bucket` part of `s3v/bucket`). The `s3v` volume itself is implicitly handled by the S3 Gateway.
- Make sure the S3 Gateway container (`s3g`) is up and running. You can check using `docker ps`.

## Troubleshooting Tips

- **Access Denied or Bucket Not Found**: Ensure that the bucket name exists and matches exactly (Ozone S3 Gateway uses flat bucket names).
- **Connection Refused**: Check that the S3 Gateway container is running and accessible at the specified endpoint.
- **Timeout or DNS Issues**: If you adapt this script to run from your host machine (outside Docker), you might need to replace s3g:9878 with localhost:9878 (assuming default port mapping).

## References

- [Apache Ozone Docker](https://github.com/apache/ozone-docker)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Ozone S3 Docs](https://ozone.apache.org/docs/edge/interface/s3.html)
- [Ozone Securing S3 Docs](https://ozone.apache.org/docs/edge/security/securings3.html)
- [Ozone Client Interfaces](https://ozone.apache.org/docs/edge/interface.html)
