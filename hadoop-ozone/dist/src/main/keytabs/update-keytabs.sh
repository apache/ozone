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
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
set -ex

export_keytab() {
   kadmin.local -q "ktadd -norandkey -k /etc/security/keytabs/$2.keytab $1@EXAMPLE.COM"
}

#this section supposed to be executed inside a docker image which already
#has these credentials
#
# the rest of the code executes this part inside a container
#
if [ "$1" == "internal" ]; then
   rm /etc/security/keytabs/*.keytab

   export_keytab scm/scm scm
   export_keytab HTTP/scm scm
   export_keytab testuser/scm scm
   export_keytab testuser2/scm scm

   export_keytab testuser/scm testuser
   export_keytab testuser/s3g testuser
   export_keytab testuser/om testuser

   export_keytab testuser2/scm testuser2

   export_keytab om/om om
   export_keytab HTTP/om om

   export_keytab s3g/s3g s3g
   export_keytab HTTP/s3g s3g
   export_keytab testuser/s3g s3g

   export_keytab recon/recon recon
   export_keytab HTTP/recon recon

   export_keytab dn/dn dn
   export_keytab HTTP/dn dn

   export_keytab HTTP/scm HTTP
   export_keytab HTTP/s3g HTTP
   export_keytab HTTP/ozone HTTP

   export_keytab hadoop/rm hadoop

   export_keytab rm/rm rm
   export_keytab nm/nm nm
   export_keytab jhs/jhs jhs



   chmod 755 /etc/security/keytabs/*.keytab
   chown 1000. /etc/security/keytabs/*.keytab
   exit 0
fi

TESTKRB5_IMAGE=$(mvn -f "$SCRIPT_DIR"/../../../pom.xml help:evaluate -Dexpression=docker.ozone-testkr5b.image -q -DforceStdout)

docker run -it --entrypoint=/etc/security/keytabs/update-keytabs.sh -v "$SCRIPT_DIR":/etc/security/keytabs $TESTKRB5_IMAGE internal


