#!/bin/bash

VERSION="1.3.0.7.3.1.100-2"

FILE=( \
hadoop-ozone/common/target/ozone-common-${VERSION}.jar \
hadoop-hdds/container-service/target/hdds-container-service-${VERSION}.jar \
hadoop-hdds/framework/target/hdds-server-framework-${VERSION}.jar \
hadoop-hdds/common/target/hdds-common-${VERSION}.jar \
hadoop-hdds/client/target/hdds-client-${VERSION}.jar \
hadoop-ozone/ozonefs-hadoop3/target/ozone-filesystem-hadoop3-${VERSION}.jar
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
