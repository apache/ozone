---
title: "Accessing Apache Ozone from Python"
date: "2025-06-02"
weight: 6
menu:
  main:
    parent: "Client Interfaces"
summary: Access Apache Ozone from Python using PyArrow, Boto3, requests and fsspec WebHDFS libraries
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

Apache Ozone project itself does not provide Python client libraries.
However, several third-party open source libraries can be used to build applications to access an Ozone cluster via
different interfaces: OFS file system, Ozone HTTPFS REST API and Ozone S3.

This document outlines these approaches, providing concise setup instructions and validated code examples.

## Setup and Prerequisites

Before starting, ensure the following:

- Python installed (3.x recommended)
- Apache Ozone configured and accessible
- For PyArrow with libhdfs:
  - PyArrow library (`pip install pyarrow`)
  - Hadoop native libraries configured and Ozone classpath specified (see below for details)
- For S3 access:
  - Boto3 (`pip install boto3`)
  - Ozone S3 Gateway endpoint and bucket names
  - Access credentials (AWS-like key and secret)
- For HttpFS access:
  - Requests (`pip install requests`) or fsspec (`pip install fsspec`)

## Method 1: Access Ozone via PyArrow and libhdfs

This approach leverages PyArrow's HadoopFileSystem API, which requires libhdfs.so native library.
The libhdfs.so is not packaged within PyArrow and you must download it separately from Hadoop.

### Configuration
Ensure Ozone configuration files (core-site.xml and ozone-site.xml) are available and `OZONE_CONF_DIR` is set.
Also ensure `ARROW_LIBHDFS_DIR` and `CLASSPATH` are set properly.

For example,
```shell
export ARROW_LIBHDFS_DIR=hadoop-3.4.0/lib/native/
export CLASSPATH=$(ozone classpath ozone-tools)
```

### Code Example
```python
import pyarrow.fs as pafs

# Connect to Ozone using HadoopFileSystem
# "default" tells PyArrow to use the fs.defaultFS property from core-site.xml
fs = pafs.HadoopFileSystem("default")

# Create a directory inside the bucket
fs.create_dir("volume/bucket/aaa")

# Write data to a file
path = "volume/bucket/file1"
with fs.open_output_stream(path) as stream:
  stream.write(b'data')
```
> **Note:** configure fs.defaultFS in the core-site.xml to point to the Ozone cluster. For example,
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>ofs://om:9862</value>
    <description>Ozone Manager endpoint</description>
  </property>
</configuration>
```

Try it yourself! Check out [PyArrow Tutorial](../recipe/PyArrowTutorial.md) for a quick start using Ozone's Docker image.

## Method 2: Access Ozone via Boto3 and S3 Gateway

### Configuration
- Identify your Ozone S3 Gateway endpoint (e.g., `http://s3g:9878`).
- Use AWS-compatible credentials (from Ozone).

### Code Example
```python
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
> **Note:** Replace endpoint URL, credentials, and bucket names with your setup.

Try it yourself! Check out [Boto3 Tutorial](../recipe/Boto3Tutorial.md) for a quick start using Ozone's Docker image.


## Method 3: Access Ozone via HttpFS REST API
First, install requests Python module:

```shell
pip install requests
```

### Configuration
- Use Ozone’s HTTPFS endpoint (e.g., `http://httpfs:14000`).

### Code Example (requests)
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

Try it yourself! Check out [Access Ozone using HTTPFS REST API Tutorial](../recipe/PythonRequestsOzoneHttpFS.md) for a quick start using Ozone's Docker image.


### Code Example (webhdfs)

First, install fsspec Python module:

```shell
pip install fsspec
```

```python
from fsspec.implementations.webhdfs import WebHDFS

fs = WebHDFS(host='httpfs', port=14000, user='ozone')

# Read a file from /vol1/bucket1/hello.txt
file_path = "/vol1/bucket1/hello.txt"

with fs.open(file_path, mode='rb') as f:
  content = f.read()
  print("File contents:")
  print(content.decode('utf-8'))
```
> **Note:** Replace host, port, and path as per your setup.

## Troubleshooting Tips

- **Authentication Errors**: Verify credentials and Kerberos tokens (if used).
- **Connection Issues**: Check endpoint URLs, ports, and firewall rules.
- **FileSystem Errors**: Ensure correct Ozone configuration and appropriate permissions.
- **Missing Dependencies**: Install required Python packages (`pip install pyarrow boto3 requests fsspec`).

## References and Further Resources

- [Apache Ozone Documentation](https://ozone.apache.org/docs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [fsspec WebHDFS Python API](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.webhdfs.WebHDFS)
