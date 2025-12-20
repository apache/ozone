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

FROM apache/ozone-runner:@docker.ozone-runner.version@

ADD --chown=hadoop . /opt/hadoop

WORKDIR /opt/hadoop

RUN rm -rf docs tests smoketest compose kubernetes share/ozone/byteman && \
    rm -f share/ozone/lib/ozone-filesystem-hadoop2*.jar \
        share/ozone/lib/ozone-filesystem-hadoop3*.jar \
        share/ozone/lib/ozone-filesystem-hadoop3-client*.jar \
        share/ozone/lib/hdds-docs*.jar \
        bom.json bom.xml \
        README.md HISTORY.md SECURITY.md CONTRIBUTING.md
