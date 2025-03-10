#!/bin/bash
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

source .env

downloadIfNotPresent() {
  local fileName=$1
  local urlBase=$2

  if [ ! -f "./../../share/${fileName}" ]
  then
    echo "downloading ${urlBase}/${fileName}.."

    curl -L "${urlBase}/${fileName}" --output ./../../share/${fileName}
  else
    echo "file already in cache: ${fileName}"
  fi
}

downloadIfNotPresent ranger-${RANGER_VERSION}-ozone-plugin.tar.gz "https://www.apache.org/dyn/closer.lua?action=download&filename=ranger/${RANGER_VERSION}/plugins/ozone"

if [ ! -d ./../../share/ranger-${RANGER_VERSION}-ozone-plugin ]
then
  mkdir -p ./../../share/ranger-${RANGER_VERSION}-ozone-plugin
  tar xvfz ./../../share/ranger-${RANGER_VERSION}-ozone-plugin.tar.gz -C ./../../share/ranger-${RANGER_VERSION}-ozone-plugin --strip-components 1
else
  echo "ranger-${RANGER_VERSION}-ozone-plugin directory exists already!"
fi

cp -f ranger-plugin/install.properties        ./../../share/ranger-${RANGER_VERSION}-ozone-plugin/
cp -f ranger-plugin/ranger-plugin-install.sh  ./../../share/ranger-${RANGER_VERSION}-ozone-plugin/
cp -f ranger-plugin/enable-ozone-plugin.sh    ./../../share/ranger-${RANGER_VERSION}-ozone-plugin/

chmod +x ./../../share/ranger-${RANGER_VERSION}-ozone-plugin/ranger-plugin-install.sh
chmod +x ./../../share/ranger-${RANGER_VERSION}-ozone-plugin/enable-ozone-plugin.sh

echo "copied files successfully!"
