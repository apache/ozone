---
title: "Recon"
weight: 7
menu:
   main:
      parent: Features
summary: Recon is the Web UI and analysis service for Ozone
---
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
-->

Recon is the Web UI and analytics service for Ozone. It's an optional component, but strongly recommended as it can add additional visibility.

Recon collects all the data from an Ozone cluster and **store** them in a SQL database for further analyses.

 1. Ozone Manager data is downloaded in the background by an async process. A RocksDB snapshots are created on OM side periodically, and the incremental data is copied to Recon and processed.
 2. Datanodes can send Heartbeats not just to SCM but Recon. Recon can be a read-only listener of the Heartbeats and updates the local database based on the received information.   