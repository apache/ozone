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

# For Legacy Bucket Operations

For Legacy buckets, set `ozone.om.enable.filesystem.paths` to `true` for them to behave like FSO buckets, 
otherwise Legacy buckets act like OBS buckets.

This is the same as `compose/ozone` but for testing operations that need `ozone.om.enable.filesystem.paths`
flag enabled.