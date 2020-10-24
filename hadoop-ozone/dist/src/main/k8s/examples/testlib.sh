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

retry() {
   n=0
   until [ $n -ge 100 ]
   do
      "$@" && break
      n=$[$n+1]
      echo "$n '$@' is failed..."
      sleep ${RETRY_SLEEP:-3}
   done
   if [ $n -eq 100 ]; then
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
   retry all_pods_are_running
   retry grep_log scm-0 "SCM exiting safe mode."
   retry grep_log om-0 "HTTP server of ozoneManager listening"
   print_phase "Cluster is up and running"
}

all_pods_are_running() {
   RUNNING_COUNT=$(kubectl get pod --field-selector status.phase=Running | wc -l)
   ALL_COUNT=$(kubectl get pod | wc -l)
   RUNNING_COUNT=$((RUNNING_COUNT - 1))
   ALL_COUNT=$((ALL_COUNT - 1))
   if [ "$RUNNING_COUNT" -lt "3" ]; then
      echo "$RUNNING_COUNT pods are running. Waiting for more."
      return 1
   elif [ "$RUNNING_COUNT" -ne "$ALL_COUNT" ]; then
      echo "$RUNNING_COUNT pods are running out from the $ALL_COUNT"
      return 2
   else
      STARTED=true
      return 0
   fi
}

start_k8s_env() {
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

   print_phase "Applying k8s resources from $1"
   kubectl apply -f .
   wait_for_startup
}

get_logs() {
  mkdir -p logs
  for pod in $(kubectl get pods -o custom-columns=NAME:.metadata.name | tail -n +2); do
    kubectl logs "${pod}" > "logs/pod-${pod}.log"
  done
}

stop_k8s_env() {
   if [ ! "$KEEP_RUNNING" ]; then
     kubectl delete -f .
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
    OZONE_ROOT=$(realpath ../../../../../target/ozone-0.6.0-SNAPSHOT)
  else
    #running from dist
    OZONE_ROOT=$(realpath ../../..)
  fi

  flekszible generate -t mount:hostPath="$OZONE_ROOT",path=/opt/hadoop -t image:image=apache/ozone-runner:20200420-1 -t ozone/onenode
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
  rm result/output.xml || true
  rebot -d result --nostatusrc -o output.xml -N $(basename "$(pwd)") result/*.xml
}

print_phase() {
   echo ""
   echo "**** $1 ****"
   echo ""
}
