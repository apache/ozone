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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
RESULT_DIR=result
#delete previous results
rm -rf "${DIR:?}/$RESULT_DIR"
mkdir -p "$DIR/$RESULT_DIR"
#Should be writeable from the docker containers where user is different.
chmod ogu+w "$DIR/$RESULT_DIR"

execute_tests(){
  COMPOSE_DIR=$1
  COMPOSE_FILE=$DIR/../compose/$COMPOSE_DIR/docker-compose.yaml
  TESTS=$2
  echo "-------------------------------------------------"
  echo "Executing test(s): [${TESTS[*]}]"
  echo ""
  echo "  Cluster type:      $COMPOSE_DIR"
  echo "  Compose file:      $COMPOSE_FILE"
  echo "  Output dir:        $DIR/$RESULT_DIR"
  echo "  Command to rerun:  ./test.sh --keep --env $COMPOSE_DIR $TESTS"
  echo "-------------------------------------------------"
  docker-compose -f "$COMPOSE_FILE" down
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "Waiting 30s for cluster start up..."
  sleep 30
  for TEST in "${TESTS[@]}"; do
     TITLE="Ozone $TEST tests with $COMPOSE_DIR cluster"
     set +e
     OUTPUT_NAME="$COMPOSE_DIR-${TEST//\//_}"
	  docker-compose -f "$COMPOSE_FILE" exec datanode python -m robot --log NONE --report NONE "${OZONE_ROBOT_OPTS[@]}" --output "smoketest/$RESULT_DIR/robot-$OUTPUT_NAME.xml" --logtitle "$TITLE" --reporttitle "$TITLE" "smoketest/$TEST"
     set -e
     docker-compose -f "$COMPOSE_FILE" logs > "$DIR/$RESULT_DIR/docker-$OUTPUT_NAME.log"
  done
  if [ "$KEEP_RUNNING" = false ]; then
     docker-compose -f "$COMPOSE_FILE" down
  fi
}
RUN_ALL=true
KEEP_RUNNING=false
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --env)
    DOCKERENV="$2"
    RUN_ALL=false
    shift # past argument
    shift # past value
    ;;
    --keep)
    KEEP_RUNNING=true
    shift # past argument
    ;;
    --help|-h|-help)
    cat << EOF

 Acceptance test executor for ozone.

 This is a lightweight test executor for ozone.

 You can run it with

     ./test.sh

 Which executes all the tests in all the available environments.

 Or you can run manually one test with

 ./test.sh --keep --env ozone-hdfs basic

     --keep  means that docker cluster won't be stopped after the test (optional)
     --env defines the subdirectory under the compose dir
     The remaining parameters define the test suites under smoketest dir.
     Could be any directory or robot file relative to the smoketest dir.
EOF
    exit 0
    ;;
    *)
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done

if [ "$RUN_ALL" = true ]; then
#
# This is the definition of the ozone acceptance test suite
#
# We select the test suites and execute them on multiple type of clusters
#
   DEFAULT_TESTS=("basic")
   execute_tests ozone "${DEFAULT_TESTS[@]}"
   TESTS=("ozonefs")
   execute_tests ozonefs "${TESTS[@]}"
   TESTS=("s3")
   execute_tests ozones3 "${TESTS[@]}"
else
   execute_tests "$DOCKERENV" "${POSITIONAL[@]}"
fi

#Generate the combined output and return with the right exit code (note: robot = execute test, rebot = generate output)
docker run --rm -it -v "$DIR/..:/opt/hadoop" apache/hadoop-runner rebot -d "smoketest/$RESULT_DIR" "smoketest/$RESULT_DIR/robot-*.xml"
