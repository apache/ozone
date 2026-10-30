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
#
# If you have Hadoop sources locally, set HADOOP_AWS_DIR to the directory containing `hadoop-aws`.
# Otherwise it will try to download Hadoop ${HADOOP_VERSION}, defaults to ${hadoop.version}.

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

: "${HADOOP_AWS_DIR:=${TEMP_DIR:-/tmp}/hadoop-aws-${HADOOP_VERSION}}"
export HADOOP_AWS_DIR

download_hadoop_aws() {
  if [[ ! -e "${HADOOP_AWS_DIR}" ]] || [[ ! -d "${HADOOP_AWS_DIR}"/src/test/resources ]]; then
    if [[ "${SKIP_APACHE_VERIFY_DOWNLOAD:-}" != "true" ]]; then
      import_keys https://downloads.apache.org/hadoop/common/KEYS
    fi

    (
      tarball="$(download_hadoop)" \
        && mkdir -p "${HADOOP_AWS_DIR}" \
        && tar -x -z -C "${HADOOP_AWS_DIR}" --strip-components=3 -f "${tarball}" \
            --wildcards 'hadoop-*-src/hadoop-tools/hadoop-aws'
    ) || return 1
    # keep tarball for caching
  fi
}

if ! download_hadoop_aws; then
  echo "[ERROR] Failed to download Hadoop"
  exit 1
fi

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
    -Dscan.value.testset=s3a \
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
