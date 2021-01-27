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

# Compose file for restart

This directory contains a sample cluster definition and script for
testing restart of the cluster with the same version.

Data for each container is persisted in mounted volume (by default it's
`data` under the `compose/restart` directory, but can be overridden via
`OZONE_VOLUME` environment variable).
