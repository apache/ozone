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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/../../smoketest/testlib.sh"

retry() {
   local -i n=0
   local -i attempts=${RETRY_ATTEMPTS:-100}
   until [ $n -ge $attempts ]
   do
      "$@" && break
      n=$[$n+1]
      echo "$n '$@' is failed..."
      sleep ${RETRY_SLEEP:-3}
   done
   if [ $n -eq $attempts ]; then
      return 255
   fi
}

grep_log() {
   CONTAINER="$1"
   PATTERN="$2"
   kubectl logs "$1"  | grep "$PATTERN"
}

wait_for_startup(){
   print_phase "Waiting until the k8s cluster is running"
   if retry all_pods_are_running \
       && retry grep_log scm-0 "SCM exiting safe mode." \
       && retry grep_log om-0 "HTTP server of ozoneManager listening"; then
     print_phase "Cluster is up and running"
   else
     return 1
   fi
}

all_pods_are_running() {
   local -i running=$(kubectl get pod --field-selector status.phase=Running | grep -v 'STATUS' | wc -l)
   local -i all=$(kubectl get pod | grep -v 'STATUS' | wc -l)
   if [ "$running" -lt "3" ]; then
      echo "$running pods are running. Waiting for more."
      return 1
   elif [ "$running" -ne "$all" ]; then
      echo "$running / $all pods are running"
      return 2
   else
      STARTED=true
      return 0
   fi
}

pre_run_setup() {
  rm -fr logs result
  regenerate_resources
  reset_k8s_env
  start_k8s_env
  wait_for_startup
}

reset_k8s_env() {
   print_phase "Deleting existing k8s resources"
   #reset environment
   kubectl delete statefulset --all
   kubectl delete daemonset --all
   kubectl delete deployment --all
   kubectl delete service --all
   kubectl delete configmap --all
   kubectl delete pod --all
   kubectl delete pvc --all
   kubectl delete pv --all
}

start_k8s_env() {
   print_phase "Applying k8s resources from $(basename $(pwd))"
   kubectl apply -k .
   trap post_run EXIT HUP INT TERM
}

post_run() {
  set +e
  combine_reports
  get_logs
  stop_k8s_env
  revert_resources
  set -e
}

get_logs() {
  print_phase "Collecting container logs"
  mkdir -p logs
  for pod in $(kubectl get pods -o custom-columns=NAME:.metadata.name | tail -n +2); do
    for initContainer in $(kubectl get pod -o jsonpath='{.spec.initContainers[*].name}' "${pod}"); do
      kubectl logs "${pod}" "${initContainer}" > logs/"pod-${pod}-${initContainer}.log"
    done
    kubectl logs "${pod}" > logs/"pod-${pod}.log"
  done
}

stop_k8s_env() {
   if [ "${KEEP_RUNNING:-false}" != "true" ]; then
     print_phase "Deleting k8s resources"
     kubectl delete -k .
   fi
}

regenerate_resources() {
  print_phase "Modifying Kubernetes resources file for test"
  echo "   (mounting current Ozone directory to the containers, scheduling containers to one node, ...)"
  echo ""
  echo "WARNING: this test can be executed only with local Kubernetes cluster"
  echo "   (source dir should be available from K8s nodes)"
  echo ""

  PARENT_OF_PARENT=$(realpath ../..)

  if [ $(basename $PARENT_OF_PARENT) == "k8s" ]; then
    #running from src dir
    local version
    version=$(cd ../../../../.. && mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout -Dscan=false)
    OZONE_ROOT=$(realpath ../../../../../target/ozone-${version})
  else
    #running from dist
    OZONE_ROOT=$(realpath ../../..)
  fi

  local default_version=${docker.ozone-runner.version} # set at build-time from Maven property
  local runner_version=${OZONE_RUNNER_VERSION:-${default_version}} # may be specified by user running the test
  local runner_image="${OZONE_RUNNER_IMAGE:-apache/ozone-runner}" # may be specified by user running the test

  flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image="${runner_image}:${runner_version}" -t ozone/onenode
}

revert_resources() {
   print_phase "Regenerating original Kubernetes resource files"
   flekszible generate
}

execute_robot_test() {
   print_phase "Executing robot tests $@"
   mkdir -p result

   CONTAINER="$1"
   shift 1 #Remove first argument which was the container name

   # shellcheck disable=SC2206
   ARGUMENTS=($@)

   kubectl exec -it "${CONTAINER}" -- bash -c 'rm -rf /tmp/report'
   kubectl exec -it "${CONTAINER}" -- bash -c 'mkdir -p  /tmp/report'
   kubectl exec -it "${CONTAINER}" -- robot --nostatusrc -d /tmp/report ${ARGUMENTS[@]} || true
   kubectl cp "${CONTAINER}":/tmp/report/output.xml "result/$CONTAINER-$RANDOM.xml" || true
}

combine_reports() {
  if [[ -d result ]]; then
    rm -f result/output.xml
    run_rebot result result "-o output.xml -N '$(basename $(pwd))' *.xml"
  fi
}

print_phase() {
   echo ""
   echo "**** $1 ****"
   echo ""
}
