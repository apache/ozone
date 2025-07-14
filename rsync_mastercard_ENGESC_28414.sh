#!/bin/bash

VERSION="1.4.0.7.1.9.1000-103"


FILE=( \
hadoop-hdds/common/target/hdds-common-${VERSION}.jar \
hadoop-hdds/framework/target/hdds-server-framework-${VERSION}.jar \
hadoop-ozone/common/target/ozone-common-${VERSION}.jar \
hadoop-ozone/ozone-manager/target/ozone-manager-${VERSION}.jar \
hadoop-ozone/interface-client/target/ozone-interface-client-${VERSION}.jar \
hadoop-ozone/tools/target/ozone-tools-${VERSION}.jar
)

PASSWORDLESS_USER="root"

HOSTS=("ccycloud-1.weichiu.root.comops.site" "ccycloud-2.weichiu.root.comops.site" "ccycloud-3.weichiu.root.comops.site")

for host in "${HOSTS[@]}"; do
	echo $host
	for f in "${FILE[@]}"; do
		echo $f
		rsync -raP -e 'ssh -o StrictHostKeyChecking=no' --exclude ".*" --exclude "*/.*" $f ${PASSWORDLESS_USER}@$host:/opt/cloudera/parcels/CDH/jars/
	done
done
