#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs S3A contract tests against various bucket types on
# a Docker Compose-based Ozone cluster.
# Requires HADOOP_AWS_DIR to point the directory containing hadoop-aws sources.

if [[ -z ${HADOOP_AWS_DIR} ]] || [[ ! -e ${HADOOP_AWS_DIR} ]]; then
  echo "Skipping S3A tests due to missing HADOOP_AWS_DIR (directory with hadoop-aws sources)" >&2
  exit
fi

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

## @description Run S3A contract tests against Ozone.
## @param       Ozone S3 bucket
execute_s3a_tests() {
  local bucket="$1"

  pushd "${HADOOP_AWS_DIR}"

  # S3A contract tests are enabled by presence of `auth-keys.xml`.
  # https://hadoop.apache.org/docs/r3.3.6/hadoop-aws/tools/hadoop-aws/testing.html#Setting_up_the_tests
  cat > src/test/resources/auth-keys.xml <<-EOF
  <configuration>

    <property>
      <name>fs.s3a.endpoint</name>
      <value>http://localhost:9878</value>
    </property>

    <property>
      <name>test.fs.s3a.endpoint</name>
      <value>http://localhost:9878</value>
    </property>

    <property>
      <name>fs.contract.test.fs.s3a</name>
      <value>s3a://${bucket}/</value>
    </property>

    <property>
      <name>test.fs.s3a.name</name>
      <value>s3a://${bucket}/</value>
    </property>

    <property>
      <name>fs.s3a.access.key</name>
      <value>${AWS_ACCESS_KEY_ID}</value>
    </property>

    <property>
      <name>fs.s3a.secret.key</name>
      <value>${AWS_SECRET_ACCESS_KEY}</value>
    </property>

    <property>
      <name>test.fs.s3a.sts.enabled</name>
      <value>false</value>
    </property>

    <property>
      <name>fs.s3a.committer.staging.conflict-mode</name>
      <value>replace</value>
    </property>

    <property>
      <name>fs.s3a.path.style.access</name>
      <value>true</value>
    </property>

    <property>
      <name>fs.s3a.directory.marker.retention</name>
      <value>keep</value>
    </property>

  </configuration>
EOF

  # Some tests are skipped due to known issues.
  # - ITestS3AContractBulkDelete: HDDS-11661
  # - ITestS3AContractCreate: HDDS-11663
  # - ITestS3AContractDistCp: HDDS-10616
  # - ITestS3AContractMkdirWithCreatePerf: HDDS-11662
  # - ITestS3AContractRename: HDDS-10665
  mvn ${MAVEN_ARGS:-} --fail-never --show-version \
    -Dtest='ITestS3AContract*, ITestS3ACommitterMRJob, !ITestS3AContractBulkDelete, !ITestS3AContractCreate#testOverwrite*EmptyDirectory[*], !ITestS3AContractDistCp, !ITestS3AContractMkdirWithCreatePerf, !ITestS3AContractRename' \
    clean test

  local target="${RESULT_DIR}/junit/${bucket}/target"
  mkdir -p "${target}"
  mv -iv target/surefire-reports "${target}"/
  popd
}

start_docker_env

if [[ ${SECURITY_ENABLED} == "true" ]]; then
  execute_command_in_container s3g kinit -kt /etc/security/keytabs/testuser.keytab "testuser/s3g@EXAMPLE.COM"
  access=$(execute_command_in_container s3g ozone s3 getsecret -e)
  eval "$access"
else
  export AWS_ACCESS_KEY_ID="s3a-contract"
  export AWS_SECRET_ACCESS_KEY="unsecure"
fi

execute_command_in_container s3g ozone sh bucket create --layout OBJECT_STORE /s3v/obs-bucket
execute_command_in_container s3g ozone sh bucket create --layout LEGACY /s3v/leg-bucket
execute_command_in_container s3g ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/fso-bucket

for bucket in obs-bucket leg-bucket fso-bucket; do
  execute_s3a_tests "$bucket"
done
