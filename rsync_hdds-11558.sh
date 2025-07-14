#!/bin/bash

FILE=(hadoop-ozone/common/target/ozone-common-1.3.0.7.3.1.0-165.jar \
hadoop-ozone/interface-client/target/ozone-interface-client-1.3.0.7.3.1.0-165.jar \
hadoop-ozone/ozone-manager/target/ozone-manager-1.3.0.7.3.1.0-165.jar \
hadoop-hdds/container-service/target/hdds-container-service-1.3.0.7.3.1.0-165.jar \
hadoop-ozone/ozonefs-hadoop3/target/ozone-filesystem-hadoop3-1.3.0.7.3.1.0-165.jar
)

PASSWORDLESS_USER="root"

HOSTS=("ccycloud-1.weichiu-731.root.comops.site" "ccycloud-2.weichiu-731.root.comops.site" "ccycloud-3.weichiu-731.root.comops.site")

for host in "${HOSTS[@]}"; do
	echo $host
	for f in "${FILE[@]}"; do
		echo $f
		rsync -raP -e 'ssh -o StrictHostKeyChecking=no' --exclude ".*" --exclude "*/.*" $f ${PASSWORDLESS_USER}@$host:/opt/cloudera/parcels/CDH/jars/
	done
done
