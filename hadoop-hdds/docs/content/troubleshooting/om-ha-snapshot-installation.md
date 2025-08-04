---
title: Troubleshooting OM HA snapshot installation issues
weight: 12
---
<!--
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

When a new Ozone Manager (OM) is added to an existing OM HA cluster, it needs to obtain the latest OM DB snapshot from the leader OM.
In cases where the OM DB is very large, the new OM may get stuck in a loop trying to download the snapshot.
This can happen if the leader OM purges the Raft logs associated with the snapshot before the new OM can finish downloading it.
When this happens, the new OM will have to restart the snapshot download, and the process can repeat indefinitely.

To avoid this issue, you can configure the following properties on the leader OM:

1.  Set `ozone.om.ratis.log.purge.preservation.log.num` to a high value (e.g. 1000000).
    This property controls how many Raft logs are preserved on the leader OM.
    By setting it to a high value, you can prevent the leader from purging the logs that the new OM needs to catch up. This is a more balanced approach to ensure that some logs are preserved so that they can be replicated to the slow follower (instead of installing snapshot), but if the number of logs exceeded this amount, OM leader will purge the logs to prevent disk to be full.

2.  Set `ozone.om.ratis.log.purge.upto.snapshot.index` to `false`.
    This property prevents the leader OM from purging any logs until all followers have installed the latest snapshot.
    This ensures that the new OM will have enough time to download and install the snapshot without the logs being purged. This is a more risky approach since it might cause the Raft logs to increase indefinitely when the OM follower is down for a long time, which can cause OM metadata dir to be full.

    Note: If `ozone.om.ratis.log.purge.preservation.log.num` is set to a non-zero number, it is recommended to keep `ozone.om.ratis.log.purge.upto.snapshot.index` to `true` (default value) since `ozone.om.ratis.log.purge.upto.snapshot.index` will override the preservation configuration. Therefore, these two properties should not be set together.

By tuning these two parameters, you can avoid the OM snapshot installation loop and successfully add new OMs to your HA cluster.
