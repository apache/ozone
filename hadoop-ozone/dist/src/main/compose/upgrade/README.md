<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Compose file for upgrade

This directory contains a sample cluster definition and script for
testing upgrade from previous version to the current one.

Data for each container is persisted in mounted volume (by default it's
`data` under the `compose/upgrade` directory, but can be overridden via
`OZONE_VOLUME` environment variable).

Prior version is run using an official `apache/ozone` image, while the
current version is run with the `ozone-runner` image using locally built
source code.

Currently the test script only supports a single version upgrade (eg.
from 0.5.0 to 1.0.0).
