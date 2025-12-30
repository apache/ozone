---
title: "Native C/C++ Client Access to Ozone"
description: "Use libhdfs and libo3fs to access Apache Ozone from C/C++ applications."
weight: 20
menu:
  main:
    parent: "Client Interfaces"
type: docs
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

## Components Summary

*   **libhdfs**: The standard Hadoop C API. It is a JNI bridge between C/C++ and Java FileSystem implementations.
*   **libo3fs**: Lightweight wrapper exposing a simplified API for Ozone clients. It is built on top of `libhdfs`.
*   **OFS (Ozone FileSystem)**: Java-based filesystem client used internally by `libhdfs` to interact with Ozone.

## Overview

Native C/C++ applications can access Ozone volumes and buckets using Hadoop HDFS's `libhdfs` JNI library.
As an example, Apache Impala uses this library to access Ozone.

To demonstrate, we built a simple wrapper [`libo3fs`](https://github.com/apache/hadoop-ozone/tree/master/hadoop-ozone/native-client/libo3fs) around `libhdfs`.
Applications can choose to use either `libo3fs` or directly use the low level `libhdfs` library for basic file operations.

## Architecture and Design Notes

The native client leverages the Hadoop `libhdfs` C API for low-level JNI bindings, and implements additional wrappers in `libo3fs` to support Ozone semantics. Internally:

- `libo3fs` wraps Ozone FileSystem APIs for basic file operations (read/write).
- `libhdfs` implements JNI interface.
- JNI calls link back to a JVM-based Ozone client to execute operations.

## Building the C/C++ Client Library

### Prerequisites

- Apache Ozone built from source (`ozone-dist/`)
- Hadoop with compiled `libhdfs.so`
- Java 8 or later
- Linux (kernel > 2.6.9)

The following steps assume you have Ozone built from source code, the Hadoop binary distribution downloaded and extracted,
and the environment variables `OZONE_HOME` and `HADOOP_HOME` set to their respective directories.

### 0. Download Hadoop binary distribution (optional)
```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
tar -xzf hadoop-3.4.0.tar.gz
```

### 1. Compile the Core Library & Shared Library
```bash
export JAVA_HOME=/path/to/java
export HADOOP_HOME=/path/to/hadoop
export OZONE_HOME=/path/to/ozone

cd $OZONE_HOME/hadoop-ozone/native-client/libo3fs
gcc -fPIC -pthread \
  -I$HADOOP_HOME/include \
  -g -c o3fs.c

gcc -shared -o libo3fs.so o3fs.o \
  $HADOOP_HOME/lib/native/libhdfs.so
```

### 2. Compile Sample Applications
```bash
cd $OZONE_HOME/hadoop-ozone/native-client/libo3fs-examples
gcc -fPIC -pthread -I$OZONE_HOME/hadoop-ozone/native-client/libo3fs \
  -I$HADOOP_HOME/include -g -c libo3fs_read.c
gcc -fPIC -pthread -I$OZONE_HOME/hadoop-ozone/native-client/libo3fs \
  -I$HADOOP_HOME/include -g -c libo3fs_write.c
```

### 3. Link Executables
```bash
# o3fs_read
gcc -o o3fs_read libo3fs_read.o -L$HADOOP_HOME/lib/native -lhdfs \
  -L$JAVA_HOME/lib/server -ljvm \
  -L$OZONE_HOME/hadoop-ozone/native-client/libo3fs -lo3fs -pthread

# o3fs_write
gcc -o o3fs_write libo3fs_write.o -L$HADOOP_HOME/lib/native -lhdfs \
  -L$JAVA_HOME/lib/server -ljvm \
  -L$OZONE_HOME/hadoop-ozone/native-client/libo3fs -lo3fs -pthread
```

### 4. Environment Variables

```bash
export CLASSPATH=$($OZONE_HOME/bin/ozone classpath ozone-tools)
export LD_LIBRARY_PATH=\
$HADOOP_HOME/lib/native:\
$JAVA_HOME/lib/server:\
$OZONE_HOME/hadoop-ozone/native-client/libo3fs
```


### 5. Run Sample Applications

```shell
ozone sh volume create my-volume
ozone sh bucket create /my-volume/my-bucket
```

```shell
// o3fs_write writes a 100-byte file named 'file1' using a 100-byte buffer to the 'my-bucket' bucket in the 'my-volume' volume,
// connecting to an Ozone Manager (om) on port 9862.
./o3fs_write file1 100 100 om 9862 my-bucket my-volume
```

## Limitations

- Only basic file I/O operations are supported (no directory listing, ACLs, etc.); use `libhdfs` directly for advanced features.
- JNI dependency requires compatible JVM and shared libraries
- Testing has been limited to Linux; No Windows nor Mac supported

## References

- [Ozone FileSystem Java Interface](https://ozone.apache.org/docs/edge/interface/ofs.html)
- [Hadoop libhdfs Docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/LibHdfs.html)
- [JNI and Native Libraries in Hadoop](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/NativeLibraries.html)
