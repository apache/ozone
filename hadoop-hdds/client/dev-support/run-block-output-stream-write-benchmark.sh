#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SETTINGS="${HOME}/.m2/settings.xml"
MVN=(mvn -s "${SETTINGS}" -pl hadoop-hdds/client -q clean test-compile exec:java
  -Dexec.mainClass=org.apache.hadoop.hdds.scm.storage.BlockOutputStreamWriteBenchmark
  -Dexec.classpathScope=test)

LABEL="${1:-workspace}"
shift || true

# Quick usage reminder
cat <<'EOF'
Useful system properties (pass as -Dproperty=value after the label):
  benchmark.scaling=true                  run scaling study (recommended)
  benchmark.scaling.threads=1,2,7,14,28,42,56   override thread counts
  benchmark.heapBuffer=true|false         run only one allocation mode
  benchmark.writeSize=4194304             single write size in bytes
  benchmark.checksumType=NONE             skip checksum overhead
EOF
echo

echo "Running BlockOutputStreamWriteBenchmark (label=${LABEL}) ..."
cd "${ROOT}"

# exec:java resolves dependency JARs from the local Maven repository, not from
# target/classes.  Install interface-client (gRPC stubs) and common
# (ChunkBuffer / ALLOCATE_DIRECT) so the ratis-shaded JARs in ~/.m2 are current.
mvn -s "${SETTINGS}" -pl hadoop-hdds/interface-client,hadoop-hdds/common \
  -q install -DskipTests -DskipShade -DskipRecon -DskipDocs

# exec:java runs the benchmark in Maven's own JVM, so MAVEN_OPTS applies here.
# The mock client serialises each request via request.toByteArray() without
# any Netty transport, so direct memory use is limited to the CodecBuffer
# working set (threads * poolCapacity * streamBufferSize).
export MAVEN_OPTS="${MAVEN_OPTS:-} -Xmx4g -XX:MaxDirectMemorySize=512m"
exec "${MVN[@]}" -Dbenchmark.label="${LABEL}" "$@"
