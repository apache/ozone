#!/usr/bin/env bash

retry() {
   n=0
   until [ $n -ge 30 ]
   do
      "$@" && break
      n=$[$n+1]
      echo "$n '$@' is failed..."
      sleep 3
   done
}

grep_log() {
   CONTAINER="$1"
   PATTERN="$2"
   kubectl logs "$1"  | grep "$PATTERN"
}

wait_for_startup(){
   retry all_pods_are_running
   retry grep_log scm-0 "SCM exiting safe mode."
   retry grep_log om-0 "HTTP server of ozoneManager listening"
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

all_pods_are_in_running_state() {
   STARTED=false
   for i in $(seq 1 30); do
      RUNNING_COUNT=$(kubectl get pod --field-selector status.phase=Running | wc -l)
      ALL_COUNT=$(kubectl get pod | wc -l)
      RUNNING_COUNT=$((RUNNING_COUNT - 1))
      ALL_COUNT=$((ALL_COUNT - 1))
      if [ "$RUNNING_COUNT" -lt "3" ]; then
         echo "$RUNNING_COUNT pods are running. Waiting for more."
      elif [ "$RUNNING_COUNT" -ne "$ALL_COUNT" ]; then
         echo "$RUNNING_COUNT pods are running out from the $ALL_COUNT"
      else
         STARTED=true
         break
      fi
      sleep 1
   done
   if [[ "$STARED" == "false" ]]; then
      echo "Pods can't be started"
      kubect get pod
      exit -1
   fi
   echo "All set let's tart to execute the robot tests."
}

start_k8s_env() {
   kubectl apply -f "$1"
   wait_for_startup
}

stop_k8s_env() {
   if [ ! "$KEEP_RUNNING" ]; then
     kubectl delete -f "$1"
   fi
}


execute_robot_test() {
   CONTAINER="$1-0"
   TEST="$2"
   PREFIX=""
   kubectl exec -it "${PREFIX}${CONTAINER}" -- bash -c 'rm -rf /tmp/report'
   kubectl exec -it "${PREFIX}${CONTAINER}" -- bash -c 'mkdir -p  /tmp/report'
   kubectl exec -it "${PREFIX}${CONTAINER}" -- robot -d /tmp/report -v "ENDPOINT_URL:http://${PREFIX}s3g-0.${PREFIX}s3g:9878" "smoketest/${TEST}"
   kubectl cp "${PREFIX}${CONTAINER}":/tmp/report report
}

