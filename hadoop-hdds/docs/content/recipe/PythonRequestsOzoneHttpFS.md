---
title: Access Ozone using HTTPFS REST API (Docker + Python Requests)
linkTitle: HTTPFS Access (Docker)
summary: Step-by-step tutorial for accessing Apache Ozone using the HTTPFS REST API via Python's requests library in a Docker-based environment.
weight: 13
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

This tutorial demonstrates how to access Apache Ozone using the HTTPFS REST API and Python’s `requests` library. It covers writing and reading a file via simple authentication.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.x environment

## Steps

### 1️⃣ Start Ozone in Docker

Download the latest Docker Compose file for Ozone and start the cluster with 3 DataNodes:
```bash
curl -O https://raw.githubusercontent.com/apache/ozone-docker/latest/docker-compose.yaml
docker compose up -d --scale datanode=3
```

### 2️⃣ Create a Volume and Bucket

Connect to the SCM container:

```bash
docker exec -it <your-scm-container-name-or-id> bash
```
> Change the container id `<your-scm-container-name-or-id>` to your actual container id.

The rest of the tutorial will run on this container.

Create a volume and a bucket:

```bash
ozone sh volume create vol1
ozone sh bucket create vol1/bucket1
```

### 3️⃣ Install Required Python Library

Install the `requests` library:

```bash
pip install requests
```

### 4️⃣ Access Ozone HTTPFS via Python

Create a script (`ozone_httpfs_example.py`) with the following content:

```python
#!/usr/bin/python
import requests

# Ozone HTTPFS endpoint and file path
host = "http://httpfs:14000"
volume = "vol1"
bucket = "bucket1"
filename = "hello.txt"
path = f"/webhdfs/v1/{volume}/{bucket}/{filename}"
user = "ozone"  # can be any value in simple auth mode

# Step 1: Initiate file creation (responds with 307 redirect)
params_create = {
    "op": "CREATE",
    "overwrite": "true",
    "user.name": user
}

print("Creating file...")
resp_create = requests.put(host + path, params=params_create, allow_redirects=False)

if resp_create.status_code != 307:
    print(f"Unexpected response: {resp_create.status_code}")
    print(resp_create.text)
    exit(1)

redirect_url = resp_create.headers['Location']
print(f"Redirected to: {redirect_url}")

# Step 2: Write data to the redirected location with correct headers
headers = {"Content-Type": "application/octet-stream"}
content = b"Hello from Ozone HTTPFS!\n"

resp_upload = requests.put(redirect_url, data=content, headers=headers)
if resp_upload.status_code != 201:
    print(f"Upload failed: {resp_upload.status_code}")
    print(resp_upload.text)
    exit(1)
print("File created successfully.")

# Step 3: Read the file back
params_open = {
    "op": "OPEN",
    "user.name": user
}

print("Reading file...")
resp_read = requests.get(host + path, params=params_open, allow_redirects=True)
if resp_read.ok:
    print("File contents:")
    print(resp_read.text)
else:
    print(f"Read failed: {resp_read.status_code}")
    print(resp_read.text)
```

Run the script:

```bash
python ozone_httpfs_example.py
```

✅ If everything is configured correctly, this will create a file in Ozone using the REST API and read it back.

## Troubleshooting Tips

- **401 Unauthorized**: Make sure `user.name` is passed as a query parameter and that proxy user settings are correct in `core-site.xml`.
- **400 Bad Request**: Add `Content-Type: application/octet-stream` to the request header.
- **Connection Refused**: Ensure `httpfs` container is running and accessible at port 14000.
- **Volume or Bucket Not Found**: Confirm you created `vol1/bucket1` in step 2.

## References

- [Apache Ozone HTTPFS Docs](https://ozone.apache.org/docs/edge/interface/httpfs.html)
- [Python requests Documentation](https://requests.readthedocs.io/)
- [Ozone Client Interfaces](https://ozone.apache.org/docs/edge/interface.html)
