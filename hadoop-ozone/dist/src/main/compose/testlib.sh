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

## @description create results directory, purging any prior data
create_results_dir() {
  #delete previous results
  rm -rf "$RESULT_DIR"
  mkdir -p "$RESULT_DIR"
  #Should be writeable from the docker containers where user is different.
  chmod ogu+w "$RESULT_DIR"
}


## @description wait until safemode exit (or 90 seconds)
## @param the docker-compose file
wait_for_safemode_exit(){
  local compose_file=$1

  #Reset the timer
  SECONDS=0

  #Don't give it up until 90 seconds
  while [[ $SECONDS -lt 90 ]]; do

     #This line checks the safemode status in scm
     local command="ozone scmcli safemode status"
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
  docker-compose -f "$COMPOSE_FILE" --no-ansi up -d --scale datanode="${datanode_count}" \
    && wait_for_safemode_exit "$COMPOSE_FILE"

  if [[ $? -gt 0 ]]; then
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
  OUTPUT_PATH="$RESULT_DIR_INSIDE/robot-$OUTPUT_NAME.xml"
  docker-compose -f "$COMPOSE_FILE" exec -T "$CONTAINER" mkdir -p "$RESULT_DIR_INSIDE"
  # shellcheck disable=SC2068
  docker-compose -f "$COMPOSE_FILE" exec -T -e  SECURITY_ENABLED="${SECURITY_ENABLED}" "$CONTAINER" python -m robot ${ARGUMENTS[@]} --log NONE -N "$TEST_NAME" --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "$OUTPUT_PATH" "$SMOKETEST_DIR_INSIDE/$TEST"

  FULL_CONTAINER_NAME=$(docker-compose -f "$COMPOSE_FILE" ps | grep "_${CONTAINER}_" | head -n 1 | awk '{print $1}')
  docker cp "$FULL_CONTAINER_NAME:$OUTPUT_PATH" "$RESULT_DIR/"
  set -e

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


## @description  Stops a docker-compose based test environment (with saving the logs)
stop_docker_env(){
  docker-compose -f "$COMPOSE_FILE" --no-ansi logs > "$RESULT_DIR/docker-$OUTPUT_NAME.log"
  if [ "${KEEP_RUNNING:-false}" = false ]; then
     docker-compose -f "$COMPOSE_FILE" --no-ansi down
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
