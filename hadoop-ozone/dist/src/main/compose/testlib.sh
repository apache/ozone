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
set -e -o pipefail

_testlib_this="${BASH_SOURCE[0]}"
_testlib_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

COMPOSE_ENV_NAME=$(basename "$COMPOSE_DIR")
RESULT_DIR=${RESULT_DIR:-"$COMPOSE_DIR/result"}
RESULT_DIR_INSIDE="/tmp/smoketest/$(basename "$COMPOSE_ENV_NAME")/result"

OM_HA_PARAM=""
if [[ -n "${OM_SERVICE_ID}" ]] && [[ "${OM_SERVICE_ID}" != "om" ]]; then
  OM_HA_PARAM="--om-service-id=${OM_SERVICE_ID}"
fi

source ${_testlib_dir}/compose_v2_compatibility.sh

: ${SCM:=scm}

## @description create results directory, purging any prior data
create_results_dir() {
  #delete previous results
  [[ "${OZONE_KEEP_RESULTS:-}" == "true" ]] || rm -rf "$RESULT_DIR"
  mkdir -p "$RESULT_DIR"
}

## @description find all the test*.sh scripts in the immediate child dirs
all_tests_in_immediate_child_dirs() {
  find . -mindepth 2 -maxdepth 2 -name 'test*.sh' | cut -c3- | sort
}

## @description Find all test*.sh scripts in immediate child dirs,
## @description applying OZONE_ACCEPTANCE_SUITE or OZONE_TEST_SELECTOR filter.
find_tests(){
  if [[ -n "${OZONE_ACCEPTANCE_SUITE}" ]]; then
    tests=$(all_tests_in_immediate_child_dirs | xargs grep -l "^#suite:${OZONE_ACCEPTANCE_SUITE}$" || echo "")

     # 'misc' is default suite, add untagged tests, too
    if [[ "misc" == "${OZONE_ACCEPTANCE_SUITE}" ]]; then
      untagged="$(all_tests_in_immediate_child_dirs | xargs grep -L "^#suite:")"
      if [[ -n "${untagged}" ]]; then
        tests=$(echo ${tests} ${untagged} | xargs -n1 | sort)
      fi
    fi

    if [[ -z "${tests}" ]]; then
      echo "No tests found for suite ${OZONE_ACCEPTANCE_SUITE}"
      exit 1
    fi
  elif [[ -n "${OZONE_TEST_SELECTOR}" ]]; then
    tests=$(all_tests_in_immediate_child_dirs | grep "${OZONE_TEST_SELECTOR}" || echo "")

    if [[ -z "${tests}" ]]; then
      echo "No tests found for filter ${OZONE_TEST_SELECTOR}"
      exit 1
    fi
  else
    tests=$(all_tests_in_immediate_child_dirs | xargs grep -L '^#suite:failing')
  fi
  echo $tests
}

## @description wait until safemode exit (or 240 seconds)
wait_for_safemode_exit(){
  local cmd="ozone admin safemode wait -t 240"
  if [[ "${SECURITY_ENABLED}" == 'true' ]]; then
    wait_for_port kdc 88 60
    cmd="kinit -k HTTP/scm@EXAMPLE.COM -t /etc/security/keytabs/HTTP.keytab && $cmd"
  fi

  wait_for_port ${SCM} 9860 120
  execute_commands_in_container ${SCM} "$cmd"
}

## @description wait until OM leader is elected (or 120 seconds)
wait_for_om_leader() {
  if [[ -z "${OM_SERVICE_ID:-}" ]]; then
    echo "No OM HA service, no need to wait"
    return
  fi

  #Reset the timer
  SECONDS=0

  #Don't give it up until 120 seconds
  while [[ $SECONDS -lt 120 ]]; do
    local command="ozone admin om getserviceroles --service-id '${OM_SERVICE_ID}'"
    if [[ "${SECURITY_ENABLED}" == 'true' ]]; then
      status=$(docker-compose exec -T ${SCM} bash -c "kinit -k scm/scm@EXAMPLE.COM -t /etc/security/keytabs/scm.keytab && $command" | grep LEADER || true)
    else
      status=$(docker-compose exec -T ${SCM} bash -c "$command" | grep LEADER || true)
    fi
    if [[ -n "${status}" ]]; then
      echo "Found OM leader for service ${OM_SERVICE_ID}: $status"

      local grep_command="grep -e FOLLOWER -e LEADER | sort -r -k3 | awk '{ print \$1 }' | xargs echo | sed 's/ /,/g'"
      local new_order
      if [[ "${SECURITY_ENABLED}" == 'true' ]]; then
        new_order=$(docker-compose exec -T ${SCM} bash -c "kinit -k scm/scm@EXAMPLE.COM -t /etc/security/keytabs/scm.keytab && $command | ${grep_command}")
      else
        new_order=$(docker-compose exec -T ${SCM} bash -c "$command | ${grep_command}")
      fi

      reorder_om_nodes "${new_order}"
      return
    else
      echo "Waiting for OM leader for service ${OM_SERVICE_ID}"
    fi

    echo "SECONDS: $SECONDS"

    sleep 2
  done
  echo "WARNING: OM leader still not found for service ${OM_SERVICE_ID}"
  return 1
}

## @description  Starts a docker-compose based test environment
## @param number of datanodes to start and wait for (default: 3)
start_docker_env(){
  local -i datanode_count=${1:-3}

  create_results_dir
  export OZONE_SAFEMODE_MIN_DATANODES="${datanode_count}"

  docker-compose --ansi never down

  trap stop_docker_env EXIT HUP INT TERM

  opts=""
  if has_scalable_datanode; then
    opts="--scale datanode=${datanode_count}"
  fi

  docker-compose --ansi never up -d $opts

  wait_for_safemode_exit
  wait_for_om_leader
}

has_scalable_datanode() {
  local files="${COMPOSE_FILE:-docker-compose.yaml}"
  local oifs=${IFS}
  local rc=1
  IFS=:
  for f in ${files}; do
    if [[ -e "${f}" ]] && grep -q '^\s*datanode:' ${f}; then
      rc=0
      break
    fi
  done
  IFS=${oifs}
  return ${rc}
}

## @description  Execute robot tests in a specific container.
## @param        Name of the container in the docker-compose file
## @param        robot test file or directory relative to the smoketest dir
execute_robot_test(){
  CONTAINER="$1"
  shift 1 #Remove first argument which was the container name
  # shellcheck disable=SC2206
  ARGUMENTS=($@)
  TEST="${ARGUMENTS[${#ARGUMENTS[@]}-1]}" #Use last element as the test name
  unset 'ARGUMENTS[${#ARGUMENTS[@]}-1]' #Remove the last element, remainings are the custom parameters
  TEST_NAME=$(basename "$TEST")
  TEST_NAME="$(basename "$COMPOSE_DIR")-${TEST_NAME%.*}"

  local output_name=$(get_output_name)

  # find unique filename
  declare -i i=0
  OUTPUT_FILE="robot-${output_name}1.xml"
  while [[ -f $RESULT_DIR/$OUTPUT_FILE ]]; do
    let ++i
    OUTPUT_FILE="robot-${output_name}${i}.xml"
  done

  SMOKETEST_DIR_INSIDE="${OZONE_DIR:-/opt/hadoop}/smoketest"

  OUTPUT_PATH="$RESULT_DIR_INSIDE/${OUTPUT_FILE}"

  set +e

  # shellcheck disable=SC2068
  docker-compose exec -T "$CONTAINER" mkdir -p "$RESULT_DIR_INSIDE" \
    && docker-compose exec -T "$CONTAINER" robot \
      -v KEY_NAME:"${OZONE_BUCKET_KEY_NAME}" \
      -v OM_HA_PARAM:"${OM_HA_PARAM}" \
      -v OM_SERVICE_ID:"${OM_SERVICE_ID:-om}" \
      -v OZONE_DIR:"${OZONE_DIR}" \
      -v SECURITY_ENABLED:"${SECURITY_ENABLED}" \
      -v SCM:"${SCM}" \
      ${ARGUMENTS[@]} --log NONE --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "$OUTPUT_PATH" \
      "$SMOKETEST_DIR_INSIDE/$TEST"
  local -i rc=$?

  FULL_CONTAINER_NAME=$(docker-compose ps -a | grep "[-_]${CONTAINER}[-_]" | head -n 1 | awk '{print $1}')
  docker cp "$FULL_CONTAINER_NAME:$OUTPUT_PATH" "$RESULT_DIR/"

  if [[ ${rc} -gt 0 ]] && [[ ${rc} -le 250 ]]; then
    create_stack_dumps
  fi

  set -e

  return ${rc}
}

## @description Replace OM node order in config
reorder_om_nodes() {
  local c pid procname new_order
  local new_order="$1"

  if [[ -n "${new_order}" ]] && [[ "${new_order}" != "om1,om2,om3" ]]; then
    for c in $(docker-compose ps | cut -f1 -d' ' | grep -e datanode -e recon -e s3g -e scm); do
      docker exec "${c}" sed -i -e "s/om1,om2,om3/${new_order}/" /etc/hadoop/ozone-site.xml
      echo "Replaced OM order with ${new_order} in ${c}"
    done
  fi
}

## @description Create stack dump of each java process in each container
create_stack_dumps() {
  local c pid procname
  for c in $(docker-compose ps | cut -f1 -d' ' | grep -e datanode -e om -e recon -e s3g -e scm); do
    while read -r pid procname; do
      echo "jstack $pid > ${RESULT_DIR}/${c}_${procname}.stack"
      docker exec "${c}" bash -c "jstack $pid" > "${RESULT_DIR}/${c}_${procname}.stack"
    done < <(docker exec "${c}" sh -c "jps | grep -v Jps" || true)
  done
}

## @description Copy any 'out' files for daemon processes to the result dir
copy_daemon_logs() {
  local c f
  for c in $(docker-compose ps -a | grep "^${COMPOSE_ENV_NAME}[_-]" | awk '{print $1}'); do
    for f in $(docker exec "${c}" ls -1 /var/log/hadoop 2> /dev/null | grep -F -e '.out' -e audit); do
      docker cp "${c}:/var/log/hadoop/${f}" "$RESULT_DIR/"
    done
  done
}


## @description  Execute specific command in docker container
## @param        container name
## @param        specific command to execute
execute_command_in_container(){
  # shellcheck disable=SC2068
  docker-compose exec -T "$@"
}

## @description  Execute specific commands in docker container
## @param        container name
## @param        specific commands to execute
execute_commands_in_container(){
  local container=$1
  shift 1
  local command=$@

  # shellcheck disable=SC2068
  docker-compose exec -T $container /bin/bash -c "$command"
}

## @description Stop a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
stop_containers() {
  docker-compose --ansi never stop $@
}


## @description Start a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
start_containers() {
  docker-compose --ansi never start $@
}

create_containers() {
  docker-compose --ansi never up -d $@
}

get_output_name() {
  if [[ -n "${OUTPUT_NAME}" ]]; then
    echo "${OUTPUT_NAME}-"
  fi
}

save_container_logs() {
  local output_name=$(get_output_name)
  local id
  for i in $(docker-compose ps -a -q "$@"); do
    local c=$(docker ps -a --filter "id=${i}" --format "{{ .Names }}")
    docker logs "${i}" >> "$RESULT_DIR/docker-${output_name}${c}.log" 2>&1
  done
}


## @description wait until the port is available on the given host
## @param The host to check for the port
## @param The port to check for
## @param The maximum time to wait in seconds
wait_for_port(){
  local host=$1
  local port=$2
  local timeout=$3

  #Reset the timer
  SECONDS=0

  while [[ $SECONDS -lt $timeout ]]; do
     if docker-compose exec -T ${SCM} /bin/bash -c "nc -z $host $port"; then
       echo "Port $port is available on $host"
       return
     fi
     echo "Port $port is not available on $host yet"
     sleep 1
   done
   echo "Timed out waiting on $host $port to become available"
   return 1
}

## @description wait for the stat to be ready
## @param The container ID
## @param The maximum time to wait in seconds
## @param The command line to be executed
wait_for_execute_command(){
  local container=$1
  local timeout=$2
  shift 2
  local command=$@

  #Reset the timer
  SECONDS=0

  while [[ $SECONDS -lt $timeout ]]; do
     if docker-compose exec -T $container /bin/bash -c "$command"; then
        echo "$command succeed"
        return
     fi
        echo "$command hasn't succeed yet"
        sleep 1
   done
   echo "Timed out waiting on $command to be successful"
   return 1
}

## @description  Stops a docker-compose based test environment (with saving the logs)
stop_docker_env(){
  copy_daemon_logs
  save_container_logs
  if [ "${KEEP_RUNNING:-false}" = false ]; then
    down_repeats=3
    for i in $(seq 1 $down_repeats)
    do
      if docker-compose --ansi never down; then
        return
      fi
      if [[ ${i} -eq 1 ]]; then
        create_stack_dumps
      fi
      sleep 5
    done

    echo "Failed to remove all docker containers in $down_repeats attempts."
    return 1
  fi
}

## @description  Removes the given docker images if configured not to keep them (via KEEP_IMAGE=false)
cleanup_docker_images() {
  if [[ "${KEEP_IMAGE:-true}" == false ]]; then
    docker image rm "$@"
  fi
}

## @description  Run Robot Framework report generator (rebot) in ozone-runner container.
## @param input directory where source Robot XML files are
## @param output directory where report should be placed
## @param rebot options and arguments
run_rebot() {
  local input_dir="$(realpath "$1")"
  local output_dir="$(realpath "$2")"

  shift 2

  local tempdir="$(mktemp -d --suffix rebot -p "${output_dir}")"
  #Should be writeable from the docker containers where user is different.
  chmod a+wx "${tempdir}"
  if docker run --rm -v "${input_dir}":/rebot-input -v "${tempdir}":/rebot-output -w /rebot-input \
      $(get_runner_image_spec) \
      bash -c "rebot --nostatusrc -d /rebot-output $@"; then
    mv -v "${tempdir}"/* "${output_dir}"/
  fi
  rmdir "${tempdir}"
}

## @description  Generate robot framework reports based on the saved results.
generate_report(){
  local title="${1:-${COMPOSE_ENV_NAME}}"
  local dir="${2:-${RESULT_DIR}}"
  local xunitdir="${3:-}"

  if [[ -n "$(find "${dir}" -mindepth 1 -maxdepth 1 -name "*.xml")" ]]; then
    xunit_args=""
    if [[ -n "${xunitdir}" ]] && [[ -e "${xunitdir}" ]]; then
      xunit_args="--xunit TEST-ozone.xml"
    fi

    run_rebot "$dir" "$dir" "--reporttitle '${title}' -N '${title}' ${xunit_args} *.xml"

    if [[ -n "${xunit_args}" ]]; then
      mv -v "${dir}"/TEST-ozone.xml "${xunitdir}"/ || rm -f "${dir}"/TEST-ozone.xml
    fi
  fi
}

## @description  Copy results of a single test environment to the "all tests" dir.
copy_results() {
  local test_dir="$1"
  local all_result_dir="$2"
  local test_script="${3:-test.sh}"

  local result_dir="${test_dir}/result"
  local test_dir_name="$(basename ${test_dir})"
  local test_name="${test_dir_name}"
  local target_dir="${all_result_dir}"/"${test_dir_name}"

  if [[ -n "${test_script}" ]] && [[ "${test_script}" != "test.sh" ]]; then
    local test_script_name=${test_script}
    test_script_name=${test_script_name#test-}
    test_script_name=${test_script_name#test_}
    test_script_name=${test_script_name%.sh}
    test_name="${test_name}-${test_script_name}"
    target_dir="${target_dir}/${test_script_name}"
  fi

  if [[ -n "$(find "${result_dir}" -mindepth 1 -maxdepth 1 -name "*.xml")" ]]; then
    run_rebot "${result_dir}" "${all_result_dir}" "-N '${test_name}' -l NONE -r NONE -o '${test_name}.xml' *.xml" \
      && rm -fv "${result_dir}"/*.xml "${result_dir}"/log.html "${result_dir}"/report.html
  fi

  mkdir -pv "${target_dir}"
  mv -v "${result_dir}"/* "${target_dir}"/
}

run_test_script() {
  local d="$1"
  local test_script="${2:-test.sh}"

  echo "Executing test ${d}/${test_script}"

  #required to read the .env file from the right location
  cd "${d}" || return

  local ret=0
  if ! ./"$test_script"; then
    ret=1
    echo "ERROR: Test execution of ${d}/${test_script} is FAILED!!!!"
  fi

  cd - > /dev/null

  return ${ret}
}

run_test_scripts() {
  local ret=0
  local d f t

  for t in "$@"; do
    d="$(dirname "${t}")"
    f="$(basename "${t}")"

    if ! run_test_script "${d}" "${f}"; then
      ret=1
    fi

    copy_results "${d}" "${ALL_RESULT_DIR}" "${f}"

    if [[ "${ret}" == "1" ]] && [[ "${FAIL_FAST:-}" == "true" ]]; then
      break
    fi
  done

  return ${ret}
}

## @description Make `OZONE_VOLUME_OWNER` the owner of the `OZONE_VOLUME`
##   directory tree (required in Github Actions runner environment)
fix_data_dir_permissions() {
  if [[ -n "${OZONE_VOLUME}" ]] && [[ -n "${OZONE_VOLUME_OWNER}" ]]; then
    current_user=$(whoami)
    if [[ "${OZONE_VOLUME_OWNER}" != "${current_user}" ]]; then
      chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}" \
        || sudo chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}"
    fi
  fi
}

## @description Define variables required for using Ozone docker image which
##   includes binaries for a specific release
## @param `ozone` image version
prepare_for_binary_image() {
  local v=$1

  export OZONE_DIR=/opt/ozone
  export OZONE_IMAGE="apache/ozone:${v}"
}

## @description Define variables required for using `ozone-runner` docker image
##   (no binaries included)
## @param `ozone-runner` image version (optional)
get_runner_image_spec() {
  local default_version=${docker.ozone-runner.version} # set at build-time from Maven property
  local runner_version=${OZONE_RUNNER_VERSION:-${default_version}} # may be specified by user running the test
  local runner_image=${OZONE_RUNNER_IMAGE:-apache/ozone-runner} # may be specified by user running the test
  local v=${1:-${runner_version}} # prefer explicit argument

  echo "${runner_image}:${v}"
}

## @description Define variables required for using `ozone-runner` docker image
##   (no binaries included)
## @param `ozone-runner` image version (optional)
prepare_for_runner_image() {
  export OZONE_DIR=/opt/hadoop
  export OZONE_IMAGE="$(get_runner_image_spec "$@")"
}

## @description Executing the Ozone Debug CLI related robot tests
execute_debug_tests() {
  local prefix=${RANDOM}

  local volume="cli-debug-volume${prefix}"
  local bucket="cli-debug-bucket"
  local key="testfile"

  execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests.robot

  # get block locations for key
  local chunkinfo="${key}-blocks-${prefix}"
  docker-compose exec -T ${SCM} bash -c "ozone debug chunkinfo ${volume}/${bucket}/${key}" > "$chunkinfo"
  local host="$(jq -r '.KeyLocations[0][0]["Datanode-HostName"]' ${chunkinfo})"
  local container="${host%%.*}"

  # corrupt the first block of key on one of the datanodes
  local datafile="$(jq -r '.KeyLocations[0][0].Locations.files[0]' ${chunkinfo})"
  docker exec "${container}" sed -i -e '1s/^/a/' "${datafile}"

  execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "CORRUPT_DATANODE:${host}" debug/ozone-debug-corrupt-block.robot

  docker stop "${container}"

  wait_for_datanode "${container}" STALE 60
  execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "STALE_DATANODE:${host}" debug/ozone-debug-stale-datanode.robot

  wait_for_datanode "${container}" DEAD 60
  execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-dead-datanode.robot

  docker start "${container}"

  wait_for_datanode "${container}" HEALTHY 60
}

## @description  Wait for datanode state
## @param        Datanode name, eg datanode_1 datanode_2
## @param        State to check for
## @param        The maximum time to wait in seconds
wait_for_datanode() {
  local datanode=$1
  local state=$2
  local timeout=$3

  SECONDS=0
  while [[ $SECONDS -lt $timeout ]]; do
    local command="ozone admin datanode list"
    docker-compose exec -T ${SCM} bash -c "$command" | grep -A2 "$datanode" > /tmp/dn_check
    local health=$(grep -c "State: $state" /tmp/dn_check)

    if [[ "$health" -eq 1 ]]; then
      echo "$datanode is $state"
      return
    else
      echo "Waiting for $datanode to be $state"
    fi
    echo "SECONDS: $SECONDS"
  done
  echo "WARNING: $datanode is still not $state"
}


## @description wait for n root certificates
wait_for_root_certificate(){
  local container=$1
  local timeout=$2
  local count=$3
  local command="ozone admin cert list --role=scm -c 100 | grep -v "scm-sub" | grep "scm" | wc -l"

  #Reset the timer
  SECONDS=0
  while [[ $SECONDS -lt $timeout ]]; do
    cert_number=`docker-compose exec -T $container /bin/bash -c "$command"`
    if [[ $cert_number -eq $count ]]; then
      echo "$count root certificates are found"
      return
    fi
      echo "$count root certificates are not found yet"
      sleep 1
  done
  echo "Timed out waiting on $count root certificates. Current timestamp " $(date +"%T")
  return 1
}
