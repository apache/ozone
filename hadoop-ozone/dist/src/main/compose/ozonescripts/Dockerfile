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

ARG OZONE_RUNNER_IMAGE
ARG OZONE_RUNNER_VERSION

FROM ${OZONE_RUNNER_IMAGE}:${OZONE_RUNNER_VERSION}

RUN sudo dnf install -y openssh-clients openssh-server

RUN sudo ssh-keygen -A
RUN sudo mkdir -p /run/sshd
RUN sudo sed -i "s/.*UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config
RUN sudo sed -i "s/.*PermitUserEnvironment.*/PermitUserEnvironment yes/g" /etc/ssh/sshd_config
RUN sudo sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN sudo rm -f /etc/nologin /var/run/nologin

#/opt/hadoop is mounted, we can't use it as a home
RUN sudo usermod -d /opt hadoop
ADD .ssh /opt/.ssh
RUN sudo chown -R hadoop /opt/.ssh
RUN sudo chown hadoop /opt
RUN sudo chmod 600 /opt/.ssh/*
RUN sudo chmod 700 /opt/.ssh

RUN echo "export JAVA_HOME=$JAVA_HOME" | sudo sh -c 'cat >> /etc/profile'
RUN echo "JAVA_HOME=$JAVA_HOME" >> /opt/.ssh/environment
RUN echo "OZONE_CONF_DIR=$OZONE_CONF_DIR" >> /opt/.ssh/environment
RUN echo "OZONE_LOG_DIR=$OZONE_LOG_DIR" >> /opt/.ssh/environment

CMD ["sudo","/usr/sbin/sshd","-D"]
