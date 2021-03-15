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

The scripts in this directory define version-specific behavior required for [`testlib.sh`](../testlib.sh).  For example the `ozone admin` command was renamed from `ozone scmcli` in 1.0.0.

For each release a logical version is defined, starting from 1.  It is incremented only if the new version needs to execute some scripts to handle some (unintended) incompatibility.  For example 1.0.0 or later need to rename some RocksDB column-families when upgrading from 0.5.0.

Interface:

 * `ozone_logical_version`: print the logical version
 * `ozone_version_load`: define version-specific variables for the test library
 * `ozone_version_unload`: unset version-specific variables; this reverts test library behavior to the "current" one.
