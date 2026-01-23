---
title: Access Ozone using PyArrow (Docker Quickstart)
linkTitle: PyArrow Access (Docker)
summary: Step-by-step tutorial for accessing Ozone from Python using PyArrow in a Docker environment.
weight: 11
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

This tutorial demonstrates how to access Apache Ozone from Python using **PyArrow**, with Ozone running in Docker.

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

Create a volume and a bucket inside Ozone:

```bash
ozone sh volume create volume
ozone sh bucket create volume/bucket
```

### 3️⃣ Install PyArrow in Your Python Environment

```bash
pip install pyarrow
```

### 4️⃣ Download Hadoop Native Libraries for libhdfs Support

Depending on your system architecture, run one of the following:

For ARM64 (Apple Silicon, ARM servers):
```bash
curl -L "https://www.apache.org/dyn/closer.lua?action=download&filename=hadoop/common/hadoop-3.4.0/hadoop-3.4.0-aarch64.tar.gz" | tar -xz --wildcards 'hadoop-3.4.0/lib/native/libhdfs.*'
```

For x86_64 (most desktops and servers):
```bash
curl -L "https://www.apache.org/dyn/closer.lua?action=download&filename=hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz" | tar -xz --wildcards 'hadoop-3.4.0/lib/native/libhdfs.*'
```

Set environment variables to point to the native libraries and Ozone classpath:

```bash
export ARROW_LIBHDFS_DIR=hadoop-3.4.0/lib/native/
export CLASSPATH=$(ozone classpath ozone-tools)
```

### 5️⃣ Configure Core-Site.xml

Add the following to `/etc/hadoop/core-site.xml`:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>ofs://om:9862</value>
        <description>Ozone Manager endpoint</description>
    </property>
</configuration>
```
> Note: the Docker container has environment variable `OZONE_CONF_DIR=/etc/hadoop/` so it knows where to locate the configuration files.

### 6️⃣ Access Ozone Using PyArrow

Create a Python script (`ozone_pyarrow_example.py`) with the following code:

```python
#!/usr/bin/python
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

Run the script:

```bash
python ozone_pyarrow_example.py
```

✅ Congratulations! You’ve successfully accessed Ozone from Python using PyArrow and Docker.

## Troubleshooting Tips

- **libhdfs Errors**: Ensure `ARROW_LIBHDFS_DIR` is set and points to the correct native library path.
- **Connection Issues**: Verify the Ozone Manager endpoint (`om:9862`) is correct and reachable.
- **Permissions**: Ensure your Ozone user has the correct permissions for the volume and bucket.

## References

- [Apache Ozone Docker](https://github.com/apache/ozone-docker)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [PyArrow HadoopFileSystem Reference](https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html)
- [Ozone Client Interfaces](https://ozone.apache.org/docs/edge/interface.html)
