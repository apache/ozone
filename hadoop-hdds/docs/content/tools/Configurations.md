---
title: "Ozone configurations"
summary: Ozone configurations
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

| **Name**        | `hadoop.hdds.db.rocksdb.WAL_size_limit_MB` |
|:----------------|:----------------------------|
| **Value**       | 0MB |
| **Tag**         | OM, SCM, DATANODE |
| **Description** | The total size limit of WAL log files. Once the total log file size exceeds this limit, the earliest files will be deleted.Default 0 means no limit. |
--------------------------------------------------------------------------------
| **Name**        | `hadoop.hdds.db.rocksdb.WAL_ttl_seconds` |
|:----------------|:----------------------------|
| **Value**       | 1200 |
| **Tag**         | OM, SCM, DATANODE |
| **Description** | The lifetime of WAL log files. Default 1200 seconds. |
--------------------------------------------------------------------------------
| **Name**        | `hadoop.hdds.db.rocksdb.logging.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OM, SCM, DATANODE |
| **Description** | Enable/Disable RocksDB logging for OM. |
--------------------------------------------------------------------------------
| **Name**        | `hadoop.hdds.db.rocksdb.logging.level` |
|:----------------|:----------------------------|
| **Value**       | INFO |
| **Tag**         | OM, SCM, DATANODE |
| **Description** | OM RocksDB logging level (INFO/DEBUG/WARN/ERROR/FATAL) |
--------------------------------------------------------------------------------
| **Name**        | `hadoop.hdds.db.rocksdb.writeoption.sync` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OM, SCM, DATANODE |
| **Description** | Enable/Disable Sync option. If true write will be considered complete, once flushed to persistent storage. If false, writes are flushed asynchronously. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.balancing.iteration.interval` |
|:----------------|:----------------------------|
| **Value**       | 70m |
| **Tag**         | BALANCER |
| **Description** | The interval period between each iteration of Container Balancer. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.datanodes.involved.max.percentage.per.iteration` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | BALANCER |
| **Description** | Maximum percentage of healthy, in service datanodes that can be involved in balancing in one iteration. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.exclude.containers` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | BALANCER |
| **Description** | List of container IDs to exclude from balancing. For example "1, 4, 5" or "1,4,5". |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.exclude.datanodes` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | BALANCER |
| **Description** | A list of Datanode hostnames or ip addresses separated by commas. The Datanodes specified in this list are excluded from balancing. This configuration is empty by default. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.include.datanodes` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | BALANCER |
| **Description** | A list of Datanode hostnames or ip addresses separated by commas. Only the Datanodes specified in this list are balanced. This configuration is empty by default and is applicable only if it is non-empty. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.iterations` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | BALANCER |
| **Description** | The number of iterations that Container Balancer will run for. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.move.networkTopology.enable` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | BALANCER |
| **Description** | whether to take network topology into account when selecting a target for a source. This configuration is false by default. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.move.replication.timeout` |
|:----------------|:----------------------------|
| **Value**       | 50m |
| **Tag**         | BALANCER |
| **Description** | The amount of time to allow a single container's replication from source to target as part of container move. For example, if "hdds.container.balancer.move.timeout" is 65 minutes, then out of those 65 minutes 50 minutes will be the deadline for replication to complete. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.move.timeout` |
|:----------------|:----------------------------|
| **Value**       | 65m |
| **Tag**         | BALANCER |
| **Description** | The amount of time to allow a single container to move from source to target. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.size.entering.target.max` |
|:----------------|:----------------------------|
| **Value**       | 26GB |
| **Tag**         | BALANCER |
| **Description** | The maximum size that can enter a target datanode in each iteration while balancing. This is the sum of data from multiple sources. The value must be greater than the configured (or default) ozone.scm.container.size. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.size.leaving.source.max` |
|:----------------|:----------------------------|
| **Value**       | 26GB |
| **Tag**         | BALANCER |
| **Description** | The maximum size that can leave a source datanode in each iteration while balancing. This is the sum of data moving to multiple targets. The value must be greater than the configured (or default) ozone.scm.container.size. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.size.moved.max.per.iteration` |
|:----------------|:----------------------------|
| **Value**       | 500GB |
| **Tag**         | BALANCER |
| **Description** | The maximum size of data in bytes that will be moved by Container Balancer in one iteration. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.trigger.du.before.move.enable` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | BALANCER |
| **Description** | whether to send command to all the healthy and in-service data nodes to run du immediately before startinga balance iteration. note that running du is very time consuming , especially when the disk usage rate of a data node is very high |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.balancer.utilization.threshold` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | BALANCER |
| **Description** | Threshold is a percentage in the range of 0 to 100. A cluster is considered balanced if for each datanode, the utilization of the datanode (used space to capacity ratio) differs from the utilization of the cluster (used space to capacity ratio of the entire cluster) no more than the threshold. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.data.scan.interval` |
|:----------------|:----------------------------|
| **Value**       | 7d |
| **Tag**         | STORAGE |
| **Description** | Minimum time interval between two iterations of container data scanning. If an iteration takes less time than this, the scanner will wait before starting the next iteration. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.dev.data.scan.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | STORAGE |
| **Description** | Can be used to disable the background container data scanner for developer testing purposes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.dev.metadata.scan.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | STORAGE |
| **Description** | Can be used to disable the background container metadata scanner for developer testing purposes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | STORAGE |
| **Description** | Config parameter to enable all container scanners. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.metadata.scan.interval` |
|:----------------|:----------------------------|
| **Value**       | 3h |
| **Tag**         | STORAGE |
| **Description** | Config parameter define time interval between two metadata scans by container scanner. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.min.gap` |
|:----------------|:----------------------------|
| **Value**       | 15m |
| **Tag**         | DATANODE |
| **Description** | The minimum gap between two successive scans of the same container. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.on.demand.volume.bytes.per.second` |
|:----------------|:----------------------------|
| **Value**       | 5242880 |
| **Tag**         | STORAGE |
| **Description** | Config parameter to throttle I/O bandwidth used by the demand container scanner per volume. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.scrub.volume.bytes.per.second` |
|:----------------|:----------------------------|
| **Value**       | 5242880 |
| **Tag**         | STORAGE |
| **Description** | Config parameter to throttle I/O bandwidth used by scanner per volume. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.delete.command.worker.interval` |
|:----------------|:----------------------------|
| **Value**       | 2s |
| **Tag**         | DATANODE |
| **Description** | The interval between DeleteCmdWorker execution of delete commands. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.delete.max.lock.wait.timeout` |
|:----------------|:----------------------------|
| **Value**       | 100ms |
| **Tag**         | DATANODE, DELETION |
| **Description** | Timeout for the thread used to process the delete block command to wait for the container lock. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.delete.queue.limit` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of block delete commands queued on a datanode, This configuration is also used by the SCM to control whether to send delete commands to the DN. If the DN has more commands waiting in the queue than this value, the SCM will not send any new block delete commands. until the DN has processed some commands and the queue length is reduced. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.delete.threads.max` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of threads used to handle delete blocks on a datanode |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.deleting.limit.per.interval` |
|:----------------|:----------------------------|
| **Value**       | 5000 |
| **Tag**         | SCM, DELETION |
| **Description** | Number of blocks to be deleted in an interval. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.deleting.max.lock.holding.time` |
|:----------------|:----------------------------|
| **Value**       | 1s |
| **Tag**         | DATANODE, DELETION |
| **Description** | This configuration controls the maximum time that the block deleting service can hold the lock during the deletion of blocks. Once this configured time period is reached, the service will release and re-acquire the lock. This is not a hard limit as the time check only occurs after the completion of each transaction, which means the actual execution time may exceed this limit. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.block.deleting.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | SCM, DELETION |
| **Description** | Time interval of the Datanode block deleting service. The block deleting service runs on Datanode periodically and deletes blocks queued for deletion. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.check.empty.container.dir.on.delete` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | DATANODE |
| **Description** | Boolean Flag to decide whether to check container directory or not to determine container is empty |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.chunk.data.validation.check` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | DATANODE |
| **Description** | Enable safety checks such as checksum validation for Ratis calls. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.command.queue.limit` |
|:----------------|:----------------------------|
| **Value**       | 5000 |
| **Tag**         | DATANODE |
| **Description** | The default maximum number of commands in the queue and command type's sub-queue on a datanode |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.container.close.threads.max` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of threads used to close containers on a datanode |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.container.delete.threads.max` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of threads used to delete containers on a datanode |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.container.schema.v3.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | DATANODE |
| **Description** | Enable use of container schema v3(one rocksdb per disk). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.container.schema.v3.key.separator` |
|:----------------|:----------------------------|
| **Value**       | | |
| **Tag**         | DATANODE |
| **Description** | The default separator between Container ID and container meta key name. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.df.refresh.period` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | DATANODE |
| **Description** | Disk space usage information will be refreshed with thespecified period following the completion of the last check. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.disk.check.io.failures.tolerated` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | DATANODE |
| **Description** | The number of IO tests out of the last hdds.datanode.disk.check.io.test.count test run that are allowed to fail before the volume is marked as failed. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.disk.check.io.file.size` |
|:----------------|:----------------------------|
| **Value**       | 100B |
| **Tag**         | DATANODE |
| **Description** | The size of the temporary file that will be synced to the disk and read back to assess its health. The contents of the file will be stored in memory during the duration of the check. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.disk.check.io.test.count` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | DATANODE |
| **Description** | The number of IO tests required to determine if a disk has failed. Each disk check does one IO test. The volume will be failed if more than hdds.datanode.disk.check.io.failures.tolerated out of the last hdds.datanode.disk.check.io.test.count runs failed. Set to 0 to disable disk IO checks. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.disk.check.min.gap` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | DATANODE |
| **Description** | The minimum gap between two successive checks of the same Datanode volume. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.disk.check.timeout` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | DATANODE |
| **Description** | Maximum allowed time for a disk check to complete. If the check does not complete within this time interval then the disk is declared as failed. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.du.factory.classname` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | DATANODE |
| **Description** | The fully qualified name of the factory class that creates objects for providing disk space usage information. It should implement the SpaceUsageCheckFactory interface. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.du.refresh.period` |
|:----------------|:----------------------------|
| **Value**       | 1h |
| **Tag**         | DATANODE |
| **Description** | Disk space usage information will be refreshed with thespecified period following the completion of the last check. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.failed.data.volumes.tolerated` |
|:----------------|:----------------------------|
| **Value**       | -1 |
| **Tag**         | DATANODE |
| **Description** | The number of data volumes that are allowed to fail before a datanode stops offering service. Config this to -1 means unlimited, but we should have at least one good volume left. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.failed.db.volumes.tolerated` |
|:----------------|:----------------------------|
| **Value**       | -1 |
| **Tag**         | DATANODE |
| **Description** | The number of db volumes that are allowed to fail before a datanode stops offering service. Config this to -1 means unlimited, but we should have at least one good volume left. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.failed.metadata.volumes.tolerated` |
|:----------------|:----------------------------|
| **Value**       | -1 |
| **Tag**         | DATANODE |
| **Description** | The number of metadata volumes that are allowed to fail before a datanode stops offering service. Config this to -1 means unlimited, but we should have at least one good volume left. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.periodic.disk.check.interval.minutes` |
|:----------------|:----------------------------|
| **Value**       | 60 |
| **Tag**         | DATANODE |
| **Description** | Periodic disk check run interval in minutes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.read.chunk.threads.per.volume` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | DATANODE |
| **Description** | Number of threads per volume that Datanode will use for reading replicated chunks. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.recovering.container.scrubbing.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | SCM, DELETION |
| **Description** | Time interval of the stale recovering container scrubbing service. The recovering container scrubbing service runs on Datanode periodically and deletes stale recovering container Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.outofservice.limit.factor` |
|:----------------|:----------------------------|
| **Value**       | 2.0 |
| **Tag**         | DATANODE, SCM |
| **Description** | Decommissioning and maintenance nodes can handle morereplication commands than in-service nodes due to reduced load. This multiplier determines the increased queue capacity and executor pool size. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.port` |
|:----------------|:----------------------------|
| **Value**       | 9886 |
| **Tag**         | DATANODE, MANAGEMENT |
| **Description** | Port used for the server2server replication server |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.queue.limit` |
|:----------------|:----------------------------|
| **Value**       | 4096 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of queued requests for container replication |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.streams.limit` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | DATANODE |
| **Description** | The maximum number of replication commands a single datanode can execute simultaneously |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.zerocopy.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | DATANODE, SCM |
| **Description** | Specify if zero-copy should be enabled for replication protocol. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.auto-compaction-small-sst-file` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | DATANODE |
| **Description** | Auto compact small SST files (rocksdb.auto-compaction-small-sst-file-size-threshold) when count exceeds (rocksdb.auto-compaction-small-sst-file-num-threshold) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.auto-compaction-small-sst-file-num-threshold` |
|:----------------|:----------------------------|
| **Value**       | 512 |
| **Tag**         | DATANODE |
| **Description** | Auto compaction will happen if the number of small SST files exceeds this threshold. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.auto-compaction-small-sst-file-size-threshold` |
|:----------------|:----------------------------|
| **Value**       | 1MB |
| **Tag**         | DATANODE |
| **Description** | SST files smaller than this configuration will be auto compacted. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.delete-obsolete-files-period` |
|:----------------|:----------------------------|
| **Value**       | 1h |
| **Tag**         | DATANODE |
| **Description** | Periodicity when obsolete files get deleted. Default is 1h. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.log.level` |
|:----------------|:----------------------------|
| **Value**       | INFO |
| **Tag**         | DATANODE |
| **Description** | The user log level of RocksDB(DEBUG/INFO/WARN/ERROR/FATAL)) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.log.max-file-num` |
|:----------------|:----------------------------|
| **Value**       | 64 |
| **Tag**         | DATANODE |
| **Description** | The max user log file number to keep for each RocksDB |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.log.max-file-size` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | DATANODE |
| **Description** | The max size of each user log file of RocksDB. O means no size limit. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.rocksdb.max-open-files` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | DATANODE |
| **Description** | The total number of files that a RocksDB can open. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.wait.on.all.followers` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | DATANODE |
| **Description** | Defines whether the leader datanode will wait for bothfollowers to catch up before removing the stateMachineData from the cache. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.prometheus.endpoint.token` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY, MANAGEMENT |
| **Description** | Allowed authorization token while using prometheus servlet endpoint. This will disable SPNEGO based authentication on the endpoint. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.exponential.backoff.base.sleep` |
|:----------------|:----------------------------|
| **Value**       | 4s |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Specifies base sleep for exponential backoff retry policy. With the default base sleep of 4s, the sleep duration for ith retry is min(4 * pow(2, i), max_sleep) * r, where r is random number in the range [0.5, 1.5). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.exponential.backoff.max.sleep` |
|:----------------|:----------------------------|
| **Value**       | 40s |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | The sleep duration obtained from exponential backoff policy is limited by the configured max sleep. Refer dfs.ratis.client.exponential.backoff.base.sleep for further details. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.multilinear.random.retry.policy` |
|:----------------|:----------------------------|
| **Value**       | 5s, 5, 10s, 5, 15s, 5, 20s, 5, 25s, 5, 60s, 10 |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Specifies multilinear random retry policy to be used by ratis client. e.g. given pairs of number of retries and sleep time (n0, t0), (n1, t1), ..., for the first n0 retries sleep duration is t0 on average, the following n1 retries sleep duration is t1 on average, and so on. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.request.watch.timeout` |
|:----------------|:----------------------------|
| **Value**       | 3m |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Timeout for ratis client watch request. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.request.watch.type` |
|:----------------|:----------------------------|
| **Value**       | ALL_COMMITTED |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Desired replication level when Ozone client's Raft client calls watch(), ALL_COMMITTED or MAJORITY_COMMITTED. MAJORITY_COMMITTED increases write performance by reducing watch() latency when an Ozone datanode is slow in a pipeline, at the cost of potential read latency increasing due to read retries to different datanodes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.request.write.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Timeout for ratis client write request. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.retry.policy` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.ratis.retrypolicy.RequestTypeDependentRetryPolicyCreator |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | The class name of the policy for retry. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.retrylimited.max.retries` |
|:----------------|:----------------------------|
| **Value**       | 180 |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Number of retries for ratis client request. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.client.retrylimited.retry.interval` |
|:----------------|:----------------------------|
| **Value**       | 1s |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Interval between successive retries for a ratis client request. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.client.async.outstanding-requests.max` |
|:----------------|:----------------------------|
| **Value**       | 32 |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Controls the maximum number of outstanding async requests that can be handled by the Standalone as well as Ratis client. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.client.rpc.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | The timeout duration for ratis client request (except for watch request). It should be set greater than leader election timeout in Ratis. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.client.rpc.watch.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 180s |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | The timeout duration for ratis client watch request. Timeout for the watch API in Ratis client to acknowledge a particular request getting replayed to all servers. It is highly recommended for the timeout duration to be strictly longer than Ratis server watch timeout (hdds.ratis.raft.server.watch.timeout) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.grpc.flow.control.window` |
|:----------------|:----------------------------|
| **Value**       | 5MB |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | This parameter tells how much data grpc client can send to grpc server with out receiving any ack(WINDOW_UPDATE) packet from server. This parameter should be set in accordance with chunk size. Example: If Chunk size is 4MB, considering some header size in to consideration, this can be set 5MB or greater. Tune this parameter accordingly, as when it is set with a value lesser than chunk size it degrades the ozone client performance. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.grpc.message.size.max` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | OZONE, CLIENT, PERFORMANCE |
| **Description** | Maximum message size allowed to be received by Grpc Channel (Server). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.datastream.client.pool.size` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, DATANODE, RATIS, DATASTREAM |
| **Description** | Maximum number of client proxy in NettyServerStreamRpc for datastream write. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.datastream.request.threads` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | OZONE, DATANODE, RATIS, DATASTREAM |
| **Description** | Maximum number of threads in the thread pool for datastream request. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.delete.ratis.log.directory` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | Flag to indicate whether ratis log directory will becleaned up during pipeline remove. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.leaderelection.pre-vote` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | Flag to enable/disable ratis election pre-vote. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.log.appender.wait-time.min` |
|:----------------|:----------------------------|
| **Value**       | 0us |
| **Tag**         | OZONE, DATANODE, RATIS, PERFORMANCE |
| **Description** | The minimum wait time between two appendEntries calls. In some error conditions, the leader may keep retrying appendEntries. If it happens, increasing this value to, say, 5us (microseconds) can help avoid the leader being too busy retrying. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.notification.no-leader.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | Time out duration after which StateMachine gets notified that leader has not been elected for a long time and leader changes its role to Candidate. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.rpc.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | The timeout duration of the ratis write request on Ratis Server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.rpc.slowness.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | Timeout duration after which stateMachine will be notified that follower is slow. StateMachine will close down the pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.watch.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, DATANODE, RATIS |
| **Description** | The timeout duration for watch request on Ratis Server. Timeout for the watch request in Ratis server to acknowledge a particular request is replayed to all servers. It is highly recommended for the timeout duration to be strictly shorter than Ratis client watch timeout (hdds.ratis.raft.client.rpc.watch.request.timeout). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.raft.server.write.element-limit` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OZONE, DATANODE, RATIS, PERFORMANCE |
| **Description** | Maximum number of pending requests after which the leader starts rejecting requests from client. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.server.num.snapshots.retained` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | STORAGE |
| **Description** | Config parameter to specify number of old snapshots retained at the Ratis leader. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.block.deleting.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | SCM, DELETION |
| **Description** | Time interval of the scm block deleting service. The block deletingservice runs on SCM periodically and deletes blocks queued for deletion. Unit could be defined with postfix (ns,ms,s,m,h,d). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.block.deletion.per-interval.max` |
|:----------------|:----------------------------|
| **Value**       | 100000 |
| **Tag**         | SCM, DELETION |
| **Description** | Maximum number of blocks which SCM processes during an interval. The block num is counted at the replica level.If SCM has 100000 blocks which need to be deleted and the configuration is 5000 then it would only send 5000 blocks for deletion to the datanodes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.ec.pipeline.choose.policy.impl` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy |
| **Tag**         | SCM, PIPELINE |
| **Description** | Sets the policy for choosing an EC pipeline. The value should be the full name of a class which implements org.apache.hadoop.hdds.scm.PipelineChoosePolicy. The class decides which pipeline will be used when selecting an EC Pipeline. If not set, org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy will be used as default value. One of the following values can be used: (1) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy : chooses a pipeline randomly. (2) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy : chooses a healthy pipeline randomly. (3) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.CapacityPipelineChoosePolicy : chooses the pipeline with lower utilization from two random pipelines. Note that random choose method will be executed twice in this policy.(4) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RoundRobinPipelineChoosePolicy : chooses a pipeline in a round robin fashion. Intended for troubleshooting and testing purposes only. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY |
| **Description** | The keytab file used by SCM http server to login as its service principal. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY |
| **Description** | This Kerberos principal is used when communicating to the HTTP server of SCM.The protocol used is SPNEGO. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.init.default.layout.version` |
|:----------------|:----------------------------|
| **Value**       | -1 |
| **Tag**         | SCM, UPGRADE |
| **Description** | Default Layout Version to init the SCM with. Intended to be used in tests to finalize from an older version of SCM to the latest. By default, SCM init uses the highest layout version. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.kerberos.keytab.file` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY, OZONE |
| **Description** | The keytab file used by SCM daemon to login as its service principal. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY, OZONE |
| **Description** | This Kerberos principal is used by the SCM service. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.pipeline.choose.policy.impl` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy |
| **Tag**         | SCM, PIPELINE |
| **Description** | Sets the policy for choosing a pipeline for a Ratis container. The value should be the full name of a class which implements org.apache.hadoop.hdds.scm.PipelineChoosePolicy. The class decides which pipeline will be used to find or allocate Ratis containers. If not set, org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy will be used as default value. One of the following values can be used: (1) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy : chooses a pipeline randomly. (2) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy : chooses a healthy pipeline randomly. (3) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.CapacityPipelineChoosePolicy : chooses the pipeline with lower utilization from two random pipelines. Note that random choose method will be executed twice in this policy.(4) org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RoundRobinPipelineChoosePolicy : chooses a pipeline in a round robin fashion. Intended for troubleshooting and testing purposes only. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.container.inflight.deletion.limit` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | SCM, OZONE |
| **Description** | This property is used to limit the maximum number of inflight deletion. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.container.inflight.replication.limit` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | SCM, OZONE |
| **Description** | This property is used to limit the maximum number of inflight replication. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.datanode.delete.container.limit` |
|:----------------|:----------------------------|
| **Value**       | 40 |
| **Tag**         | SCM, DATANODE |
| **Description** | A limit to restrict the total number of delete container commands queued on a datanode. Note this is intended to be a temporary config until we have a more dynamic way of limiting load |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.datanode.reconstruction.weight` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | SCM, DATANODE |
| **Description** | When counting the number of replication commands on a datanode, the number of reconstruction commands is multiplied by this weight to ensure reconstruction commands use more of the capacity, as they are more expensive to process. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.datanode.replication.limit` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | SCM, DATANODE |
| **Description** | A limit to restrict the total number of replication and reconstruction commands queued on a datanode. Note this is intended to be a temporary config until we have a more dynamic way of limiting load. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.enable.legacy` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | SCM, OZONE |
| **Description** | If true, LegacyReplicationManager will handle RATIS containers while ReplicationManager will handle EC containers. If false, ReplicationManager will handle both RATIS and EC. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.event.timeout` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | SCM, OZONE |
| **Description** | Timeout for the container replication/deletion commands sent to datanodes. After this timeout the command will be retried. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.event.timeout.datanode.offset` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | SCM, OZONE |
| **Description** | The amount of time to subtract from hdds.scm.replication.event.timeout to give a deadline on the datanodes which is less than the SCM timeout. This ensures the datanodes will not process a command after SCM believes it should have expired. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.inflight.limit.factor` |
|:----------------|:----------------------------|
| **Value**       | 0.75 |
| **Tag**         | SCM |
| **Description** | The overall replication task limit on a cluster is the number healthy nodes, times the datanode.replication.limit. This factor, which should be between zero and 1, scales that limit down to reduce the overall number of replicas pending creation on the cluster. A setting of zero disables global limit checking. A setting of 1 effectively disables it, by making the limit equal to the above equation. However if there are many decommissioning nodes on the cluster, the decommission nodes will have a higher than normal limit, so the setting of 1 may still provide some limit in extreme circumstances. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.maintenance.remaining.redundancy` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | SCM, OZONE |
| **Description** | The number of redundant containers in a group which must be available for a node to enter maintenance. If putting a node into maintenance reduces the redundancy below this value , the node will remain in the ENTERING_MAINTENANCE state until a new replica is created. For Ratis containers, the default value of 1 ensures at least two replicas are online, meaning 1 more can be lost without data becoming unavailable. For any EC container it will have at least dataNum + 1 online, allowing the loss of 1 more replica before data becomes unavailable. Currently only EC containers use this setting. Ratis containers use hdds.scm.replication.maintenance.replica.minimum. For EC, if nodes are in maintenance, it is likely reconstruction reads will be required if some of the data replicas are offline. This is seamless to the client, but will affect read performance. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.maintenance.replica.minimum` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | SCM, OZONE |
| **Description** | The minimum number of container replicas which must be available for a node to enter maintenance. If putting a node into maintenance reduces the available replicas for any container below this level, the node will remain in the entering maintenance state until a new replica is created. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.over.replicated.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | SCM, OZONE |
| **Description** | How frequently to check if there are work to process on the over replicated queue |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.push` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | SCM, DATANODE |
| **Description** | If false, replication happens by asking the target to pull from source nodes. If true, the source node is asked to push to the target node. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.thread.interval` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | SCM, OZONE |
| **Description** | There is a replication monitor thread running inside SCM which takes care of replicating the containers in the cluster. This property is used to configure the interval in which that thread runs. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.replication.under.replicated.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | SCM, OZONE |
| **Description** | How frequently to check if there are work to process on the under replicated queue |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.unknown-container.action` |
|:----------------|:----------------------------|
| **Value**       | WARN |
| **Tag**         | SCM, MANAGEMENT |
| **Description** | The action taken by SCM to process unknown containers that reported by Datanodes. The default action is just logging container not found warning, another available action is DELETE action. These unknown containers will be deleted under this action way. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scmclient.failover.max.retry` |
|:----------------|:----------------------------|
| **Value**       | 15 |
| **Tag**         | OZONE, SCM, CLIENT |
| **Description** | Max retry count for SCM Client when failover happens. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scmclient.failover.retry.interval` |
|:----------------|:----------------------------|
| **Value**       | 2s |
| **Tag**         | OZONE, SCM, CLIENT |
| **Description** | SCM Client timeout on waiting for the next connection retry to other SCM IP. The default value is set to 2 seconds. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scmclient.max.retry.timeout` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | OZONE, SCM, CLIENT |
| **Description** | Max retry timeout for SCM Client |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scmclient.rpc.timeout` |
|:----------------|:----------------------------|
| **Value**       | 15m |
| **Tag**         | OZONE, SCM, CLIENT |
| **Description** | RpcClient timeout on waiting for the response from SCM. The default value is set to 15 minutes. If ipc.client.ping is set to true and this rpc-timeout is greater than the value of ipc.ping.interval, the effective value of the rpc-timeout is rounded up to multiple of ipc.ping.interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.bytes.per.checksum` |
|:----------------|:----------------------------|
| **Value**       | 16KB |
| **Tag**         | CLIENT, CRYPTO_COMPLIANCE |
| **Description** | Checksum will be computed for every bytes per checksum number of bytes and stored sequentially. The minimum value for this config is 8KB. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.checksum.combine.mode` |
|:----------------|:----------------------------|
| **Value**       | COMPOSITE_CRC |
| **Tag**         | CLIENT |
| **Description** | The combined checksum type [MD5MD5CRC / COMPOSITE_CRC] determines which algorithm would be used to compute file checksum.COMPOSITE_CRC calculates the combined CRC of the whole file, where the lower-level chunk/block checksums are combined into file-level checksum.MD5MD5CRC calculates the MD5 of MD5 of checksums of individual chunks.Default checksum type is COMPOSITE_CRC. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.checksum.type` |
|:----------------|:----------------------------|
| **Value**       | CRC32 |
| **Tag**         | CLIENT, CRYPTO_COMPLIANCE |
| **Description** | The checksum type [NONE/ CRC32/ CRC32C/ SHA256/ MD5] determines which algorithm would be used to compute checksum for chunk data. Default checksum type is CRC32. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.datastream.buffer.flush.size` |
|:----------------|:----------------------------|
| **Value**       | 16MB |
| **Tag**         | CLIENT |
| **Description** | The boundary at which putBlock is executed |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.datastream.min.packet.size` |
|:----------------|:----------------------------|
| **Value**       | 1MB |
| **Tag**         | CLIENT |
| **Description** | The maximum size of the ByteBuffer (used via ratis streaming) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.datastream.pipeline.mode` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | CLIENT |
| **Description** | Streaming write support both pipeline mode(datanode1->datanode2->datanode3) and star mode(datanode1->datanode2, datanode1->datanode3). By default we use pipeline mode. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.datastream.window.size` |
|:----------------|:----------------------------|
| **Value**       | 64MB |
| **Tag**         | CLIENT |
| **Description** | Maximum size of BufferList(used for retry) size per BlockDataStreamOutput instance |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.reconstruct.stripe.read.pool.limit` |
|:----------------|:----------------------------|
| **Value**       | 30 |
| **Tag**         | CLIENT |
| **Description** | Thread pool max size for parallelly read available ec chunks to reconstruct the whole stripe. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.reconstruct.stripe.write.pool.limit` |
|:----------------|:----------------------------|
| **Value**       | 30 |
| **Tag**         | CLIENT |
| **Description** | Thread pool max size for parallelly write available ec chunks to reconstruct the whole stripe. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.stripe.queue.size` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | CLIENT |
| **Description** | The max number of EC stripes can be buffered in client before flushing into datanodes. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.exclude.nodes.expiry.time` |
|:----------------|:----------------------------|
| **Value**       | 600000 |
| **Tag**         | CLIENT |
| **Description** | Time after which an excluded node is reconsidered for writes. If the value is zero, the node is excluded for the life of the client |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.fs.default.bucket.layout` |
|:----------------|:----------------------------|
| **Value**       | FILE_SYSTEM_OPTIMIZED |
| **Tag**         | CLIENT |
| **Description** | The bucket layout used by buckets created using OFS. Valid values include FILE_SYSTEM_OPTIMIZED and LEGACY |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.max.ec.stripe.write.retries` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | CLIENT |
| **Description** | Ozone EC client to retry stripe to new block group on failures. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.max.retries` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | CLIENT |
| **Description** | Maximum number of retries by Ozone Client on encountering exception while writing a key |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.read.max.retries` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | CLIENT |
| **Description** | Maximum number of retries by Ozone Client on encountering connectivity exception when reading a key. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.read.retry.interval` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | CLIENT |
| **Description** | Indicates the time duration in seconds a client will wait before retrying a read key request on encountering a connectivity excepetion from Datanodes . By default the interval is 1 second |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.retry.interval` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | CLIENT |
| **Description** | Indicates the time duration a client will wait before retrying a write key request on encountering an exception. By default there is no wait |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.stream.buffer.flush.delay` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | CLIENT |
| **Description** | Default true, when call flush() and determine whether the data in the current buffer is greater than ozone.client.stream.buffer.size, if greater than then send buffer to the datanode. You can turn this off by setting this configuration to false. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.stream.buffer.flush.size` |
|:----------------|:----------------------------|
| **Value**       | 16MB |
| **Tag**         | CLIENT |
| **Description** | Size which determines at what buffer position a partial flush will be initiated during write. It should be a multiple of ozone.client.stream.buffer.size |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.stream.buffer.increment` |
|:----------------|:----------------------------|
| **Value**       | 0B |
| **Tag**         | CLIENT |
| **Description** | Buffer (defined by ozone.client.stream.buffer.size) will be incremented with this steps. If zero, the full buffer will be created at once. Setting it to a variable between 0 and ozone.client.stream.buffer.size can reduce the memory usage for very small keys, but has a performance overhead. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.stream.buffer.max.size` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | CLIENT |
| **Description** | Size which determines at what buffer position write call be blocked till acknowledgement of the first partial flush happens by all servers. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.stream.buffer.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | CLIENT |
| **Description** | The size of chunks the client will send to the server |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.verify.checksum` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | CLIENT |
| **Description** | Ozone client to verify checksum of the checksum blocksize data. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.csi.default-volume-size` |
|:----------------|:----------------------------|
| **Value**       | 1000000000 |
| **Tag**         | STORAGE |
| **Description** | The default size of the create volumes (if not specified). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.csi.mount.command` |
|:----------------|:----------------------------|
| **Value**       | goofys --endpoint %s %s %s |
| **Tag**         | STORAGE |
| **Description** | This is the mount command which is used to publish volume. these %s will be replicated by s3gAddress, volumeId and target path. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.csi.owner` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | STORAGE |
| **Description** | This is the username which is used to create the requested storage. Used as a hadoop username and the generated ozone volume used to store all the buckets. WARNING: It can be a security hole to use CSI in a secure environments as ALL the users can request the mount of a specific bucket via the CSI interface. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.csi.s3g.address` |
|:----------------|:----------------------------|
| **Value**       | http://localhost:9878 |
| **Tag**         | STORAGE |
| **Description** | The address of S3 Gateway endpoint. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.csi.socket` |
|:----------------|:----------------------------|
| **Value**       | /var/lib/csi.sock |
| **Tag**         | STORAGE |
| **Description** | The socket where all the CSI services will listen (file name). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.client.rpc.timeout` |
|:----------------|:----------------------------|
| **Value**       | 15m |
| **Tag**         | OZONE, OM, CLIENT |
| **Description** | RpcClient timeout on waiting for the response from OzoneManager. The default value is set to 15 minutes. If ipc.client.ping is set to true and this rpc-timeout is greater than the value of ipc.ping.interval, the effective value of the rpc-timeout is rounded up to multiple of ipc.ping.interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.client.trash.core.pool.size` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | OZONE, OM, CLIENT |
| **Description** | Total number of threads in pool for the Trash Emptier |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.group.rights` |
|:----------------|:----------------------------|
| **Value**       | ALL |
| **Tag**         | OM, SECURITY |
| **Description** | Default group permissions set for an object in OzoneManager. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.grpc.port` |
|:----------------|:----------------------------|
| **Value**       | 8981 |
| **Tag**         | MANAGEMENT |
| **Description** | Port used for the GrpcOmTransport OzoneManagerServiceGrpc server |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ha.raft.server.log.appender.wait-time.min` |
|:----------------|:----------------------------|
| **Value**       | 0ms |
| **Tag**         | OZONE, OM, RATIS, PERFORMANCE |
| **Description** | Minimum wait time between two appendEntries calls. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ha.raft.server.retrycache.expirytime` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | The timeout duration of the retry cache. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.init.default.layout.version` |
|:----------------|:----------------------------|
| **Value**       | -1 |
| **Tag**         | OM, UPGRADE |
| **Description** | Default Layout Version to init the OM with. Intended to be used in tests to finalize from an older version of OM to the latest. By default, OM init uses the highest layout version. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.upgrade.finalization.ratis.based.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OM, UPGRADE |
| **Description** | Maximum time to wait for a slow follower to be finalized through a Ratis snapshot. This is an advanced config, and needs to be changed only under a special circumstance when the leader OM has purged the finalize request from its logs, and a follower OM was down during upgrade finalization. Default is 30s. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.user.rights` |
|:----------------|:----------------------------|
| **Value**       | ALL |
| **Tag**         | OM, SECURITY |
| **Description** | Default user permissions set for an object in OzoneManager. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.kerberos.keytab.file` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY, RECON, OZONE |
| **Description** | The keytab file used by Recon daemon to login as its service principal. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | SECURITY, RECON, OZONE |
| **Description** | This Kerberos principal is used by the Recon service. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.security.client.datanode.container.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY, RECON, OZONE |
| **Description** | Comma separated acls (users, groups) allowing clients accessing datanode container protocol |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.auto.commit` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Sets the Ozone Recon database connection property of auto-commit to true/false. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.idle.max.age` |
|:----------------|:----------------------------|
| **Value**       | 3600s |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Sets maximum time to live for idle connection in seconds. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.idle.test` |
|:----------------|:----------------------------|
| **Value**       | SELECT 1 |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | The query to send to the DB to maintain keep-alives and test for dead connections. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.idle.test.period` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Sets maximum time to live for idle connection in seconds. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.max.active` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | The max active connections to the SQL database. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.max.age` |
|:----------------|:----------------------------|
| **Value**       | 1800s |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Sets maximum time a connection can be active in seconds. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.conn.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30000ms |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Sets time in milliseconds before call to getConnection is timed out. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.driver` |
|:----------------|:----------------------------|
| **Value**       | org.apache.derby.jdbc.EmbeddedDriver |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Recon SQL DB driver class. Defaults to Derby. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.jdbc.url` |
|:----------------|:----------------------------|
| **Value**       | jdbc:derby:${ozone.recon.db.dir}/ozone_recon_derby.db |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Ozone Recon SQL database jdbc url. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.jooq.dialect` |
|:----------------|:----------------------------|
| **Value**       | DERBY |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Recon internally uses Jooq to talk to its SQL DB. By default, we support Derby and Sqlite out of the box. Please refer to https://www.jooq.org/javadoc/latest/org.jooq/org/jooq/SQLDialect.html to specify different dialect. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.password` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Ozone Recon SQL database password. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.sql.db.username` |
|:----------------|:----------------------------|
| **Value**       | None |
| **Tag**         | STORAGE, RECON, OZONE |
| **Description** | Ozone Recon SQL database username. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.task.containercounttask.interval` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | RECON, OZONE |
| **Description** | The time interval to wait between each runs of container count task. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.task.missingcontainer.interval` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | RECON, OZONE |
| **Description** | The time interval of the periodic check for unhealthy containers in the cluster as reported by Datanodes. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.task.pipelinesync.interval` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | RECON, OZONE |
| **Description** | The time interval of periodic sync of pipeline state from SCM to Recon. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.task.safemode.wait.threshold` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | RECON, OZONE |
| **Description** | The time interval to wait for starting container health task and pipeline sync task before recon exits out of safe or warmup mode. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.replication.allowed-configs` |
|:----------------|:----------------------------|
| **Value**       | ^((STANDALONE|RATIS)/(ONE|THREE))|(EC/(3-2|6-3|10-4)-(512|1024|2048|4096)k)$ |
| **Tag**         | STORAGE |
| **Description** | Regular expression to restrict enabled replication schemes |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ec.pipeline.minimum` |
|:----------------|:----------------------------|
| **Value**       | 5 |
| **Tag**         | STORAGE |
| **Description** | The minimum number of pipelines to have open for each Erasure Coding configuration |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ec.pipeline.per.volume.factor` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | SCM |
| **Description** | TODO |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.raft.server.log.appender.wait-time.min` |
|:----------------|:----------------------------|
| **Value**       | 0ms |
| **Tag**         | OZONE, SCM, RATIS, PERFORMANCE |
| **Description** | Minimum wait time between two appendEntries calls. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.service.shutdown.timeout` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | OZONE, OM, SCM, DATANODE, RECON, S3GATEWAY |
| **Description** | Timeout to wait for each shutdown operation to completeIf a hook takes longer than this time to complete, it will be interrupted, so the service will shutdown. This allows the service shutdown to recover from a blocked operation. The minimum duration of the timeout is 1 second, if hook has been configured with a timeout less than 1 second. |
--------------------------------------------------------------------------------
| **Name**        | `scm.container.client.idle.threshold` |
|:----------------|:----------------------------|
| **Value**       | 10s |
| **Tag**         | OZONE, PERFORMANCE |
| **Description** | In the standalone pipelines, the SCM clients use netty to communicate with the container. It also uses connection pooling to reduce client side overheads. This allows a connection to stay idle for a while before the connection is closed. |
--------------------------------------------------------------------------------
| **Name**        | `scm.container.client.max.size` |
|:----------------|:----------------------------|
| **Value**       | 256 |
| **Tag**         | OZONE, PERFORMANCE |
| **Description** | Controls the maximum number of connections that are cached via client connection pooling. If the number of connections exceed this count, then the oldest idle connection is evicted. |
--------------------------------------------------------------------------------
