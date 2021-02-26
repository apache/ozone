#!/bin/sh
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

export_keytab() {
   kadmin.local -q "ktadd -norandkey -k /etc/security/keytabs/$2.keytab $1@EXAMPLE.COM"
}

export_keytab scm/scm scm
export_keytab HTTP/scm scm
export_keytab testuser/scm scm
export_keytab testuser2/scm scm

export_keytab testuser/scm testuser
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

chmod 755 /etc/security/keytabs/*.keytab
