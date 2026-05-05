<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
--->

# Ozone examples for Kubernetes

This directory contains example resources for running Ozone in Kubernetes.

Note that the files are generated based on the [definitions](../definitions) using [Flekszible](https://github.com/elek/flekszible).  If you would like to modify them permanently, edit the definition and regenerate the examples by running `regenerate-all.sh` after installing Flekszible.  As a developer, you can run `hadoop-ozone/dev-support/k8s/regenerate-examples.sh`, which will also install Flekszible.
