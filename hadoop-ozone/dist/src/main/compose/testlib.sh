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
set -e

COMPOSE_ENV_NAME=$(basename "$COMPOSE_DIR")
RESULT_DIR=${RESULT_DIR:-"$COMPOSE_DIR/result"}
RESULT_DIR_INSIDE="/tmp/smoketest/$(basename "$COMPOSE_ENV_NAME")/result"

OM_HA_PARAM=""
if [[ -n "${OM_SERVICE_ID}" ]] && [[ "${OM_SERVICE_ID}" != "om" ]]; then
  OM_HA_PARAM="--om-service-id=${OM_SERVICE_ID}"
else
  OM_SERVICE_ID=om
fi

## @description create results directory, purging any prior data
create_results_dir() {
  #delete previous results
  [[ "${OZONE_KEEP_RESULTS:-}" == "true" ]] || rm -rf "$RESULT_DIR"
  mkdir -p "$RESULT_DIR"
  #Should be writeable from the docker containers where user is different.
  chmod ogu+w "$RESULT_DIR"
}

## @description find all the test.sh scripts in the immediate child dirs
find_tests(){
  if [[ -n "${OZONE_ACCEPTANCE_SUITE}" ]]; then
     tests=$(find . -mindepth 2 -maxdepth 2 -name test.sh | xargs grep -l "^#suite:${OZONE_ACCEPTANCE_SUITE}$" | sort)

     # 'misc' is default suite, add untagged tests, too
    if [[ "misc" == "${OZONE_ACCEPTANCE_SUITE}" ]]; then
       untagged="$(find . -mindepth 2 -maxdepth 2 -name test.sh | xargs grep -L "^#suite:")"
       if [[ -n "${untagged}" ]]; then
         tests=$(echo ${tests} ${untagged} | xargs -n1 | sort)
       fi
     fi

    if [[ -z "${tests}" ]]; then
       echo "No tests found for suite ${OZONE_ACCEPTANCE_SUITE}"
       exit 1
  fi
  else
    tests=$(find . -mindepth 2 -maxdepth 2 -name test.sh | grep "${OZONE_TEST_SELECTOR:-""}" | sort)
  fi
  echo $tests
}

## @description wait until safemode exit (or 240 seconds)
wait_for_safemode_exit(){
  # version-dependent
  : ${OZONE_SAFEMODE_STATUS_COMMAND:=ozone admin safemode status --verbose}

  #Reset the timer
  SECONDS=0

  #Don't give it up until 240 seconds
  while [[ $SECONDS -lt 240 ]]; do

     #This line checks the safemode status in scm
     local command="${OZONE_SAFEMODE_STATUS_COMMAND}"
     if [[ "${SECURITY_ENABLED}" == 'true' ]]; then
         status=$(docker-compose exec -T scm bash -c "kinit -k HTTP/scm@EXAMPLE.COM -t /etc/security/keytabs/HTTP.keytab && $command" || true)
     else
         status=$(docker-compose exec -T scm bash -c "$command")
     fi

     echo "SECONDS: $SECONDS"

     echo $status
     if [[ "$status" ]]; then
       if [[ ${status} == "SCM is out of safe mode." ]]; then
         #Safemode exits. Let's return from the function.
         echo "Safe mode is off"
         return
       fi
     fi

     sleep 2
   done
   echo "WARNING! Safemode is still on. Please check the docker-compose files"
   return 1
}

## @description  Starts a docker-compose based test environment
## @param number of datanodes to start and wait for (default: 3)
start_docker_env(){
  local -i datanode_count=${1:-3}

  create_results_dir
  export OZONE_SAFEMODE_MIN_DATANODES="${datanode_count}"
  docker-compose --no-ansi down
  if ! { docker-compose --no-ansi up -d --scale datanode="${datanode_count}" \
      && wait_for_safemode_exit ; }; then
    OUTPUT_NAME="$COMPOSE_ENV_NAME"
    stop_docker_env
    return 1
  fi
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
  set +e
  OUTPUT_NAME="$COMPOSE_ENV_NAME-$TEST_NAME-$CONTAINER"

  # find unique filename
  declare -i i=0
  OUTPUT_FILE="robot-${OUTPUT_NAME}.xml"
  while [[ -f $RESULT_DIR/$OUTPUT_FILE ]]; do
    let i++
    OUTPUT_FILE="robot-${OUTPUT_NAME}-${i}.xml"
  done

  SMOKETEST_DIR_INSIDE="${OZONE_DIR:-/opt/hadoop}/smoketest"

  OUTPUT_PATH="$RESULT_DIR_INSIDE/${OUTPUT_FILE}"
  # shellcheck disable=SC2068
  docker-compose exec -T "$CONTAINER" mkdir -p "$RESULT_DIR_INSIDE" \
    && docker-compose exec -T "$CONTAINER" robot \
      -v KEY_NAME:"${OZONE_BUCKET_KEY_NAME}" \
      -v OM_HA_PARAM:"${OM_HA_PARAM}" \
      -v OM_SERVICE_ID:"${OM_SERVICE_ID}" \
      -v OZONE_DIR:"${OZONE_DIR}" \
      -v SECURITY_ENABLED:"${SECURITY_ENABLED}" \
      ${ARGUMENTS[@]} --log NONE --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "$OUTPUT_PATH" \
      "$SMOKETEST_DIR_INSIDE/$TEST"
  local -i rc=$?

  FULL_CONTAINER_NAME=$(docker-compose ps | grep "_${CONTAINER}_" | head -n 1 | awk '{print $1}')
  docker cp "$FULL_CONTAINER_NAME:$OUTPUT_PATH" "$RESULT_DIR/"

  copy_daemon_logs

  set -e

  if [[ ${rc} -gt 0 ]]; then
    stop_docker_env
  fi

  return ${rc}
}

## @description Copy any 'out' files for daemon processes to the result dir
copy_daemon_logs() {
  local c f
  for c in $(docker-compose ps | grep "^${COMPOSE_ENV_NAME}_" | awk '{print $1}'); do
    for f in $(docker exec "${c}" ls -1 /var/log/hadoop | grep -F '.out'); do
      docker cp "${c}:/var/log/hadoop/${f}" "$RESULT_DIR/"
    done
  done
}


## @description  Execute specific command in docker container
## @param        container name
## @param        specific command to execute
execute_command_in_container(){
  set -e
  # shellcheck disable=SC2068
  docker-compose exec -T "$@"
  set +e
}

## @description Stop a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
stop_containers() {
  set -e
  docker-compose --no-ansi stop $@
  set +e
}


## @description Start a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
start_containers() {
  set -e
  docker-compose --no-ansi start $@
  set +e
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
     set +e
     docker-compose exec -T scm /bin/bash -c "nc -z $host $port"
     status=$?
     set -e
     if [ $status -eq 0 ] ; then
         echo "Port $port is available on $host"
         return;
     fi
     echo "Port $port is not available on $host yet"
     sleep 1
   done
   echo "Timed out waiting on $host $port to become available"
   return 1
}


## @description  Stops a docker-compose based test environment (with saving the logs)
stop_docker_env(){
  docker-compose --no-ansi logs > "$RESULT_DIR/docker-$OUTPUT_NAME.log"
  if [ "${KEEP_RUNNING:-false}" = false ]; then
     docker-compose --no-ansi down
  fi
}

## @description  Removes the given docker images if configured not to keep them (via KEEP_IMAGE=false)
cleanup_docker_images() {
  if [[ "${KEEP_IMAGE:-true}" == false ]]; then
    docker image rm "$@"
  fi
}

## @description  Generate robot framework reports based on the saved results.
generate_report(){

  if command -v rebot > /dev/null 2>&1; then
     #Generate the combined output and return with the right exit code (note: robot = execute test, rebot = generate output)
     rebot --reporttitle "${COMPOSE_ENV_NAME}" -N "${COMPOSE_ENV_NAME}" -d "$RESULT_DIR" "$RESULT_DIR/robot-*.xml"
  else
     echo "Robot framework is not installed, the reports can be generated (sudo pip install robotframework)."
     exit 1
  fi
}

## @description  Copy results of a single test environment to the "all tests" dir.
copy_results() {
  local test_dir="$1"
  local all_result_dir="$2"

  local result_dir="${test_dir}/result"
  local test_dir_name=$(basename ${test_dir})
  if [[ -n "$(find "${result_dir}" -name "*.xml")" ]]; then
    rebot --nostatusrc -N "${test_dir_name}" -o "${all_result_dir}/${test_dir_name}.xml" "${result_dir}/*.xml"
  fi

  cp "${result_dir}"/docker-*.log "${all_result_dir}"/
  if [[ -n "$(find "${result_dir}" -name "*.out")" ]]; then
    cp "${result_dir}"/*.out* "${all_result_dir}"/
  fi
}

run_test_script() {
  local d="$1"

  echo "Executing test in ${d}"

  #required to read the .env file from the right location
  cd "${d}" || return

  ret=0
  if ! ./test.sh; then
    ret=1
    echo "ERROR: Test execution of ${d} is FAILED!!!!"
  fi

  cd - > /dev/null

  return ${ret}
}
