#!/usr/bin/env bash
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

# This script installs dependencies for the tests run by `kubernetes.sh`.

# Install k3s
curl -sfL https://get.k3s.io | sh -

# Copy Kubernetes config file
sudo mkdir ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u) ~/.kube/config

# Install flekszible
cd /tmp
wget https://github.com/elek/flekszible/releases/download/v1.8.1/flekszible_1.8.1_Linux_x86_64.tar.gz -O - | tar -zx
chmod +x flekszible
sudo mv flekszible /usr/bin/flekszible
