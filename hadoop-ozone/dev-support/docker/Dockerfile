# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM alpine
RUN apk add --update --no-cache bash alpine-sdk maven grep openjdk8 py-pip rsync procps autoconf automake libtool findutils python3-dev libffi-dev openssl-dev gcc libc-dev rust cargo make

#Install real glibc
RUN apk --no-cache add ca-certificates wget && \
    wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub && \
    wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.33-r0/glibc-2.33-r0.apk && \
    apk add glibc-2.33-r0.apk

#Spotbugs install
RUN mkdir -p /opt && \
    curl -sL https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/3.1.12/spotbugs-3.1.12.tgz | tar -xz  && \
    mv spotbugs-* /opt/spotbugs 

#Install apache-ant
RUN mkdir -p /opt && \
    curl -sL 'https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=/ant/binaries/apache-ant-1.10.11-bin.tar.gz' | tar -xz  && \
       mv apache-ant* /opt/ant

#Install docker-compose
RUN pip install docker-compose

#Install pytest==2.8.7
RUN pip install pytest==2.8.7

ENV PATH=$PATH:/opt/spotbugs/bin

RUN addgroup -g 1000 default && \
   for i in $(seq 1 2000); do adduser jenkins$i -u $i -G default -h /tmp/ -H -D; done

#This is a very huge local maven cache. Usually the mvn repository is not safe to be 
#shared between builds as concurrent installls are not handled very well
#A simple workaround is to provide all the required 3rd party lib in the docker image
#It will be cached by docker, and any additional dependency can be downloaded, artifacts
#can be installed
USER jenkins1000
RUN cd /tmp && \
   git clone --depth=1 https://gitbox.apache.org/repos/asf/ozone.git -b master && \
   cd /tmp/ozone && \
   mvn package dependency:go-offline -DskipTests -pl :ozone-dist -am && \
   rm -rf /tmp/.m2/repository/org/apache/ozone/hdds* && \
   rm -rf /tmp/.m2/repository/org/apache/ozone/ozone* && \
   find /tmp/.m2/repository -exec chmod o+wx {} \;
