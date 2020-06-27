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
COMPOSE_FILE=$COMPOSE_DIR/docker-compose.yaml
RESULT_DIR=${RESULT_DIR:-"$COMPOSE_DIR/result"}
RESULT_DIR_INSIDE="/tmp/smoketest/$(basename "$COMPOSE_ENV_NAME")/result"
SMOKETEST_DIR_INSIDE="${OZONE_DIR:-/opt/hadoop}/smoketest"

OM_HA_PARAM=""
if [[ -n "${OM_SERVICE_ID}" ]]; then
  OM_HA_PARAM="--om-service-id=${OM_SERVICE_ID}"
else
  OM_SERVICE_ID=om
fi

## @description create results directory, purging any prior data
create_results_dir() {
  #delete previous results
  rm -rf "$RESULT_DIR"
  mkdir -p "$RESULT_DIR"
  #Should be writeable from the docker containers where user is different.
  chmod ogu+w "$RESULT_DIR"
}


## @description wait until safemode exit (or 180 seconds)
## @param the docker-compose file
wait_for_safemode_exit(){
  local compose_file=$1

  #Reset the timer
  SECONDS=0

  #Don't give it up until 180 seconds
  while [[ $SECONDS -lt 180 ]]; do

     #This line checks the safemode status in scm
     local command="ozone admin safemode status"
     if [[ "${SECURITY_ENABLED}" == 'true' ]]; then
         status=$(docker-compose -f "${compose_file}" exec -T scm bash -c "kinit -k HTTP/scm@EXAMPLE.COM -t /etc/security/keytabs/HTTP.keytab && $command" || true)
     else
         status=$(docker-compose -f "${compose_file}" exec -T scm bash -c "$command")
     fi

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
  docker-compose -f "$COMPOSE_FILE" --no-ansi down
  if ! { docker-compose -f "$COMPOSE_FILE" --no-ansi up -d --scale datanode="${datanode_count}" \
      && wait_for_safemode_exit "$COMPOSE_FILE"; }; then
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

  OUTPUT_PATH="$RESULT_DIR_INSIDE/${OUTPUT_FILE}"
  # shellcheck disable=SC2068
  docker-compose -f "$COMPOSE_FILE" exec -T "$CONTAINER" mkdir -p "$RESULT_DIR_INSIDE" \
    && docker-compose -f "$COMPOSE_FILE" exec -T "$CONTAINER" robot -v OM_SERVICE_ID:"${OM_SERVICE_ID}" -v SECURITY_ENABLED:"${SECURITY_ENABLED}" -v OM_HA_PARAM:"${OM_HA_PARAM}" ${ARGUMENTS[@]} --log NONE -N "$TEST_NAME" --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "$OUTPUT_PATH" "$SMOKETEST_DIR_INSIDE/$TEST"
  local -i rc=$?

  FULL_CONTAINER_NAME=$(docker-compose -f "$COMPOSE_FILE" ps | grep "_${CONTAINER}_" | head -n 1 | awk '{print $1}')
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
  for c in $(docker-compose -f "$COMPOSE_FILE" ps | grep "^${COMPOSE_ENV_NAME}_" | awk '{print $1}'); do
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
  docker-compose -f "$COMPOSE_FILE" exec -T "$@"
  set +e
}

## @description Stop a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
stop_containers() {
  set -e
  docker-compose -f "$COMPOSE_FILE" --no-ansi stop $@
  set +e
}


## @description Start a list of named containers
## @param       List of container names, eg datanode_1 datanode_2
start_containers() {
  set -e
  docker-compose -f "$COMPOSE_FILE" --no-ansi start $@
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
     docker-compose -f "${COMPOSE_FILE}" exec -T scm /bin/bash -c "nc -z $host $port"
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
  docker-compose -f "$COMPOSE_FILE" --no-ansi logs > "$RESULT_DIR/docker-$OUTPUT_NAME.log"
  if [ "${KEEP_RUNNING:-false}" = false ]; then
     docker-compose -f "$COMPOSE_FILE" --no-ansi down
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
     rebot -d "$RESULT_DIR" "$RESULT_DIR/robot-*.xml"
  else
     echo "Robot framework is not installed, the reports can be generated (sudo pip install robotframework)."
     exit 1
  fi
}
