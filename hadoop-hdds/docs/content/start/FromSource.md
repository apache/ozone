---
title: From Source
weight: 30
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

{{< requirements >}}
* x86 or ARM64
* Linux or MacOS
* JDK 1.8 or higher
* [Maven 3.6 or later](https://maven.apache.org/download.cgi)
* Internet connection for first build (to fetch all Maven and Ozone dependencies)

You will also need to install the following dependencies to build native code (optional):
* gcc
* cmake
{{< /requirements >}}

<div class="alert alert-info" role="alert">

This is a guide on how to build the ozone sources.  If you are <font
color="red">not</font>
planning to build sources yourself, you can safely skip this page.

</div>

If you want to build from sources, Please untar the source tarball (or clone the latest code 
from the [git repository](https://github.com/apache/ozone)).

## ARM-based Linux
If you are using an ARM-based Linux, patch protobuf 2.5.0 and build it from source.

```bash
sudo yum install -y make cmake gcc g++ patch
# or for Ubuntu and Debian:
# sudo apt-get install -y make cmake gcc g++ patch

# Download protobuf 2.5.0 tarball
curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz | tar zx
cd protobuf-2.5.0

# patch protobuf 2.5.0
curl -L -O https://gist.githubusercontent.com/liusheng/64aee1b27de037f8b9ccf1873b82c413/raw/118c2fce733a9a62a03281753572a45b6efb8639/protobuf-2.5.0-arm64.patch
patch -p1 < protobuf-2.5.0-arm64.patch
# build protobuf
./configure --disable-shared
make
# install protoc to the local Maven repository
mvn install:install-file -DgroupId=com.google.protobuf -DartifactId=protoc -Dversion=2.5.0 -Dclassifier=linux-aarch_64 -Dpackaging=exe -Dfile=src/protoc
# workaround for Maven 3.9.x
cp $HOME/.m2/repository/com/google/protobuf/protoc/2.5.0/protoc-2.5.0-linux-aarch_64 $HOME/.m2/repository/com/google/protobuf/protoc/2.5.0/protoc-2.5.0-linux-aarch_64.exe
```

## ARM-based Apple silicon (Apple M1, M2, etc.)

```bash
# Patch protobuf 2.5.0 - this is needed for Hadoop 2 support
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
tar xzf protobuf-2.5.0.tar.gz
pushd protobuf-2.5.0

# Patch to insert arm64 macro
patch -p1 << EOF
diff --git a/src/google/protobuf/stubs/platform_macros.h b/src/google/protobuf/stubs/platform_macros.h
index b1df60e..8a68655 100644
--- a/src/google/protobuf/stubs/platform_macros.h
+++ b/src/google/protobuf/stubs/platform_macros.h
@@ -57,6 +57,9 @@
 #elif defined(__ppc__)
 #define GOOGLE_PROTOBUF_ARCH_PPC 1
 #define GOOGLE_PROTOBUF_ARCH_32_BIT 1
+#elif defined(__arm64__)
+#define GOOGLE_PROTOBUF_ARCH_ARM 1
+#define GOOGLE_PROTOBUF_ARCH_64_BIT 1
 #else
 #error Host architecture was not detected as supported by protobuf
 #endif
EOF

# Execute the following commands to build `protoc`
./configure --disable-shared
make -j

# Install protoc to the local Maven repository
mvn install:install-file -DgroupId=com.google.protobuf -DartifactId=protoc -Dversion=2.5.0 -Dclassifier=osx-aarch_64 -Dpackaging=exe -Dfile=src/protoc
# Workaround for Maven 3.9.x. Not needed for 3.8.x and earlier
mv $HOME/.m2/repository/com/google/protobuf/protoc/2.5.0/protoc-2.5.0-osx-aarch_64 $HOME/.m2/repository/com/google/protobuf/protoc/2.5.0/protoc-2.5.0-osx-aarch_64.exe
popd
```

## Build Ozone
Run the ozone build command.

```bash
mvn clean package -DskipTests=true
```
This will build an `ozone-\<version\>` directory in your `hadoop-ozone/dist/target` directory.

You can copy this tarball and use this instead of binary artifacts that are
provided along with the official release.

Depending on the network speed, the build can take anywhere from 10 minutes to 20 minutes.

To create tarball file distribution, use the `-Pdist` profile:

```bash
mvn clean package -DskipTests=true -Pdist
```

## Other useful Maven build options

* Use `-DskipShade` to skip shaded Ozone FS jar file creation. Saves time, but you can't test integration with other software that uses Ozone as a Hadoop-compatible file system.
* Use `-DskipRecon` to skip building Recon Web UI. It saves about 2 minutes.
* Use `-Dmaven.javadoc.skip=true` to skip building javadocs.

## How to run Ozone from build

When you have the new distribution, you can start a local cluster [with docker-compose]({{< ref "start/RunningViaDocker.md">}}).

```bash
cd hadoop-ozone/dist/target/ozone-X.X.X...
cd compose/ozone
docker-compose up -d
```

## How to test the build

`compose` subfolder contains multiple type of example setup (secure, non-secure, HA, Yarn). They can be tested with the help of [robotframework](http://robotframework.org/) with executing `test.sh` in any of the directories.
