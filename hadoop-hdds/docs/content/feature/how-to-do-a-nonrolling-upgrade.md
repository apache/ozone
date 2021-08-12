---
title: How to do a non-rolling upgrade of Ozone 
summary: Steps to do a non rolling upgrade of Ozone.
date: 2021-02-15
author: Aravindan Vijayan 
---
<!--
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

### Prepare the Ozone Manager.
 
    ozone admin om prepare -id=<om-sevice-id>

### Stop the components
Stop all components (OMs, SCMs & DNs) using an appropriate 'stop' command.

### Replace artifacts with newer version.
Replace artifacts of all components newer version.

### Start the components
Start the SCM and DNs in a regular way.
Start the Ozone Manager using the --upgrade flag.
 
    ozone --daemon om start --upgrade

**IMPORTANT** All OMs must be started with the --upgrade flag.
    - If only some are started with the flag by mistake, run `ozone admin om -id=<om-sevice-id> cancelprepare`.

### Finalize SCM and OM individually.
 
    ozone admin scm finalizeupgrade

    ozone admin om finalizeupgrade -id=<service-id>

### Downgrade (instead of finalizing)
 - Prepare the ozone managers: `ozone admin om -id=<om-sevice-id> prepare`
 - Stop all components (OMs, SCMs & DNs) using an appropriate 'stop' command.
 - Replace artifacts of all components newer version.
 - Start the SCM and DNs in a regular way.
 - Start the Ozone Manager using the '--downgrade' flag.
    - Same conditions apply to the --upgrade and --downgrade flags.

 
