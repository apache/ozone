#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

date=$(date +"%Y-%m-%d-%H-%M-%S-%Z")
logfiledirectory="/tmp/chaos-${date}/"
completesuffix="complete.log"
chaossuffix="chaos.log"
problemsuffix="problem.log"
compilesuffix="compile.log"
heapformat="dump.hprof"

#log goes to something like /tmp/2019-12-04--00-01-26-IST/complete.log
logfilename="${logfiledirectory}${completesuffix}"
#log goes to something like /tmp/2019-12-04--00-01-26-IST/chaos.log
chaosfilename="${logfiledirectory}${chaossuffix}"
#compilation log goes to something like /tmp/2019-12-04--00-01-26-IST/compile.log
compilefilename="${logfiledirectory}${compilesuffix}"
#log goes to something like /tmp/2019-12-04--00-01-26-IST/dump.hprof
heapdumpfile="${logfiledirectory}${heapformat}"
#log goes to something like /tmp/2019-12-04--00-01-26-IST/problem.log
problemfilename="${logfiledirectory}${problemsuffix}"

#TODO: add gc log file details as well
MVN_OPTS="-XX:+HeapDumpOnOutOfMemoryError "
MVN_OPTS+="-XX:HeapDumpPath=${heapdumpfile} "
MVN_OPTS+="-XX:NativeMemoryTracking=detail"
export MAVEN_OPTS=$MVN_OPTS

mkdir -p ${logfiledirectory}
echo "logging chaos logs and heapdump to ${logfiledirectory}"

echo "Starting MiniOzoneChaosCluster with ${MVN_OPTS}"
mvn clean install -DskipTests > "${compilefilename}" 2>&1
mvn exec:java \
  -Dexec.mainClass="org.apache.hadoop.ozone.OzoneChaosCluster" \
  -Dexec.classpathScope=test \
  -Dchaoslogfilename=${chaosfilename} \
  -Dproblemlogfilename=${problemfilename} \
  -Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false \
  -Dio.netty.leakDetection.level=advanced \
  -Dio.netty.leakDetectionLevel=advanced \
  -Dtest.build.data="${logfiledirectory}" \
  -Dexec.args="$*" > "${logfilename}" 2>&1
