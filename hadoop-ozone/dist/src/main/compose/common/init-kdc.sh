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

set -eux -o pipefail

# This script exports keytabs and starts KDC server.

export_keytab() {
  kadmin.local -q "addprinc -randkey $1@EXAMPLE.COM"
  kadmin.local -q "ktadd -norandkey -k /etc/security/keytabs/$2.keytab $1@EXAMPLE.COM"
}

rm -f /etc/security/keytabs/*.keytab

export_keytab scm/scm scm
export_keytab HTTP/scm scm
export_keytab testuser/scm scm
export_keytab testuser2/scm scm

export_keytab testuser/dn testuser
export_keytab testuser/httpfs testuser
export_keytab testuser/om testuser
export_keytab testuser/recon testuser
export_keytab testuser/s3g testuser
export_keytab testuser/scm testuser

export_keytab testuser2/dn testuser2
export_keytab testuser2/httpfs testuser2
export_keytab testuser2/om testuser2
export_keytab testuser2/recon testuser2
export_keytab testuser2/s3g testuser2
export_keytab testuser2/scm testuser2

export_keytab om/om om
export_keytab HTTP/om om
export_keytab testuser/om om
export_keytab testuser2/om om

export_keytab s3g/s3g s3g
export_keytab HTTP/s3g s3g
export_keytab testuser/s3g s3g
export_keytab testuser2/s3g s3g

export_keytab httpfs/httpfs httpfs
export_keytab HTTP/httpfs httpfs
export_keytab testuser/httpfs httpfs
export_keytab testuser2/httpfs httpfs

export_keytab recon/recon recon
export_keytab HTTP/recon recon
export_keytab testuser/recon recon
export_keytab testuser2/recon recon

export_keytab dn/dn dn
export_keytab HTTP/dn dn
export_keytab testuser/dn dn
export_keytab testuser2/dn dn

export_keytab HTTP/scm HTTP
export_keytab HTTP/s3g HTTP
export_keytab HTTP/httpfs HTTP
export_keytab HTTP/ozone HTTP

export_keytab hadoop/rm hadoop

export_keytab rm/rm rm
export_keytab nm/nm nm
export_keytab jhs/jhs jhs

# for Ranger
for host in dn httpfs om recon s3g scm; do
  export_keytab "hdfs/$host" hdfs
done

chmod 755 /etc/security/keytabs/*.keytab
chown 1000. /etc/security/keytabs/*.keytab

krb5kdc -n
