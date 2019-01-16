#!/usr/bin/env bash
##
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
##
set -e


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Setting up enviorment!!"

if [ -n "$KERBEROS_ENABLED" ]; then
  echo "Setting up kerberos!!"
  KERBEROS_SERVER=${KERBEROS_SERVER:-krb5}
  ISSUER_SERVER=${ISSUER_SERVER:-$KERBEROS_SERVER\:8081}

  echo "KDC ISSUER_SERVER => $ISSUER_SERVER"

  if [ -n "$SLEEP_SECONDS" ]; then
    echo "Sleeping for $(SLEEP_SECONDS) seconds"
    sleep "$SLEEP_SECONDS"
  fi


  while true
    do
      STATUS=$(curl -s -o /dev/null -w '%{http_code}' http://"$ISSUER_SERVER"/keytab/test/test)
      if [ "$STATUS" -eq 200 ]; then
        echo "Got 200, KDC service ready!!"
        break
      else
        echo "Got $STATUS :( KDC service not ready yet..."
      fi
      sleep 5
    done

    HOST_NAME=$(hostname -f)
    export HOST_NAME
    for NAME in ${KERBEROS_KEYTABS}; do
      echo "Download $NAME/$HOSTNAME@EXAMPLE.COM keytab file to $CONF_DIR/$NAME.keytab"
      wget "http://$ISSUER_SERVER/keytab/$HOST_NAME/$NAME" -O "$CONF_DIR/$NAME.keytab"
      klist -kt "$CONF_DIR/$NAME.keytab"
      KERBEROS_ENABLED=true
    done

    sed "s/SERVER/$KERBEROS_SERVER/g" "$DIR"/krb5.conf | sudo tee /etc/krb5.conf
fi

#To avoid docker volume permission problems
sudo chmod o+rwx /data

"$DIR"/envtoconf.py --destination /opt/hadoop/etc/hadoop

if [ -n "$ENSURE_NAMENODE_DIR" ]; then
  CLUSTERID_OPTS=""
  if [ -n "$ENSURE_NAMENODE_CLUSTERID" ]; then
    CLUSTERID_OPTS="-clusterid $ENSURE_NAMENODE_CLUSTERID"
  fi
  if [ ! -d "$ENSURE_NAMENODE_DIR" ]; then
    /opt/hadoop/bin/hdfs namenode -format -force "$CLUSTERID_OPTS"
  fi
fi

if [ -n "$ENSURE_STANDBY_NAMENODE_DIR" ]; then
  if [ ! -d "$ENSURE_STANDBY_NAMENODE_DIR" ]; then
    /opt/hadoop/bin/hdfs namenode -bootstrapStandby
  fi
fi

if [ -n "$ENSURE_SCM_INITIALIZED" ]; then
  if [ ! -f "$ENSURE_SCM_INITIALIZED" ]; then
    /opt/hadoop/bin/ozone scm --init
  fi
fi

if [ -n "$ENSURE_OM_INITIALIZED" ]; then
  if [ ! -f "$ENSURE_OM_INITIALIZED" ]; then
    #To make sure SCM is running in dockerized environment we will sleep
    # Could be removed after HDFS-13203
    echo "Waiting 15 seconds for SCM startup"
    sleep 15
    /opt/hadoop/bin/ozone om --init
  fi
fi

echo 'setup finished'
"$@"
