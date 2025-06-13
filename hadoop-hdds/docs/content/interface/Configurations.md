---
title: "Ozone Client Configuration Properties"
date: "2025-06-08"
weight: 10
menu:
  main:
    parent: "Client Interfaces"
summary: Configuration properties for Ozone Client.
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

The following table lists configuration properties available for the Ozone client. These properties can be set in ozone-site.xml to control client behavior.

## Ozone Client Configuration Properties

The configuration properties are generated from the Configuration class `OzoneClientConfig`.

| Property Name | Default Value | Description |
|--------------|--------------|-------------|
| ozone.client.stream.buffer.flush.size | 16MB | Size which determines at what buffer position a partial flush will be initiated during write. It should be a multiple of ozone.client.stream.buffer.size |
| ozone.client.stream.buffer.size | 4MB | The size of chunks the client will send to the server |
| ozone.client.datastream.buffer.flush.size | 16MB | The boundary at which putBlock is executed |
| ozone.client.datastream.min.packet.size | 1MB | The maximum size of the ByteBuffer (used via ratis streaming) |
| ozone.client.datastream.window.size | 64MB | Maximum size of BufferList (used for retry) size per BlockDataStreamOutput instance |
| ozone.client.datastream.pipeline.mode | true | Streaming write supports both pipeline mode (datanode1->datanode2->datanode3) and star mode (datanode1->datanode2, datanode1->datanode3). By default, pipeline mode is used. |
| ozone.client.stream.buffer.increment | 0B | Buffer (defined by ozone.client.stream.buffer.size) will be incremented with this step. If zero, the full buffer will be created at once. Setting it to a value between 0 and ozone.client.stream.buffer.size can reduce memory usage for very small keys, but has a performance overhead. |
| ozone.client.stream.buffer.flush.delay | true | Default true. When calling flush(), determines whether the data in the current buffer is greater than ozone.client.stream.buffer.size. If greater, then send buffer to the datanode. Can be turned off by setting this to false. |
| ozone.client.stream.buffer.max.size | 32MB | Size which determines at what buffer position write call will be blocked till acknowledgement of the first partial flush happens by all servers. |
| ozone.client.max.retries | 5 | Maximum number of retries by Ozone Client on encountering exception while writing a key |
| ozone.client.retry.interval | 0 | Time duration a client will wait before retrying a write key request on encountering an exception. By default there is no wait |
| ozone.client.read.max.retries | 3 | Maximum number of retries by Ozone Client on encountering connectivity exception when reading a key. |
| ozone.client.read.retry.interval | 1 | Time duration in seconds a client will wait before retrying a read key request on encountering a connectivity exception from Datanodes. By default the interval is 1 second |
| ozone.client.checksum.type | CRC32 | The checksum type [NONE/ CRC32/ CRC32C/ SHA256/ MD5] determines which algorithm would be used to compute checksum for chunk data. Default checksum type is CRC32. |
| ozone.client.bytes.per.checksum | 16KB | Checksum will be computed for every bytes per checksum number of bytes and stored sequentially. The minimum value for this config is 8KB. |
| ozone.client.verify.checksum | true | Ozone client to verify checksum of the checksum blocksize data. |
| ozone.client.max.ec.stripe.write.retries | 10 | Ozone EC client to retry stripe to new block group on failures. |
| ozone.client.ec.stripe.queue.size | 2 | The max number of EC stripes can be buffered in client before flushing into datanodes. |
| ozone.client.exclude.nodes.expiry.time | 600000 | Time after which an excluded node is reconsidered for writes. If the value is zero, the node is excluded for the life of the client |
| ozone.client.ec.reconstruct.stripe.read.pool.limit | 30 | Thread pool max size for parallel read available ec chunks to reconstruct the whole stripe. |
| ozone.client.ec.reconstruct.stripe.write.pool.limit | 30 | Thread pool max size for parallel write available ec chunks to reconstruct the whole stripe. |
| ozone.client.checksum.combine.mode | COMPOSITE_CRC | The combined checksum type [MD5MD5CRC / COMPOSITE_CRC] determines which algorithm would be used to compute file checksum. COMPOSITE_CRC calculates the combined CRC of the whole file, where the lower-level chunk/block checksums are combined into file-level checksum. MD5MD5CRC calculates the MD5 of MD5 of checksums of individual chunks. Default checksum type is COMPOSITE_CRC. |
| ozone.client.fs.default.bucket.layout | FILE_SYSTEM_OPTIMIZED | The bucket layout used by buckets created using OFS. Valid values include FILE_SYSTEM_OPTIMIZED and LEGACY |
| ozone.client.hbase.enhancements.allowed | false | When set to false, client-side HBase enhancement-related Ozone (experimental) features are disabled, overriding: `ozone.fs.hsync.enabled`, `ozone.client.incremental.chunk.list`, `ozone.client.stream.putblock.piggybacking`, and `ozone.client.key.write.concurrency`. |
| ozone.client.incremental.chunk.list | false | Client PutBlock request can choose incremental chunk list rather than full chunk list to optimize performance. Critical to HBase. EC does not support this feature. Can be enabled only when ozone.client.hbase.enhancements.allowed = true |
| ozone.client.stream.putblock.piggybacking | false | Allow PutBlock to be piggybacked in WriteChunk requests if the chunk is small. Can be enabled only when ozone.client.hbase.enhancements.allowed = true |
| ozone.client.key.write.concurrency | 1 | Maximum concurrent writes allowed on each key. Defaults to 1 which matches the behavior before HDDS-9844. For unlimited write concurrency, set this to -1 or any negative integer value. Any value other than 1 is effective only when ozone.client.hbase.enhancements.allowed = true |

## Ozone OM Client Configuration Properties

The following table lists configuration properties available for the Ozone OM client. These properties are generated from the Configuration class `OMClientConfig`.

| Property Name | Default Value | Description |
|--------------|--------------|-------------|
| ozone.om.client.rpc.timeout | 15m | RpcClient timeout on waiting for the response from OzoneManager. The default value is set to 15 minutes. If ipc.client.ping is set to true and this rpc-timeout is greater than the value of ipc.ping.interval, the effective value of the rpc-timeout is rounded up to multiple of ipc.ping.interval. |
| ozone.om.client.trash.core.pool.size | 5 | Total number of threads in pool for the Trash Emptier |

## Ozone SCM Client Configuration Properties

The following table lists configuration properties available for the Ozone SCM client. These properties are generated from the `ScmClientConfig` class (defined in XceiverClientManager.java).

| Property Name | Default Value | Description |
|--------------|--------------|-------------|
| scm.container.client.max.size | 256 | Controls the maximum number of connections that are cached via client connection pooling. If the number of connections exceed this count, then the oldest idle connection is evicted. |
| scm.container.client.idle.threshold | 10s | In the standalone pipelines, the SCM clients use netty to communicate with the container. It also uses connection pooling to reduce client side overheads. This allows a connection to stay idle for a while before the connection is closed. |

## Ozone Ratis Client Configuration Properties

The following table lists configuration properties available for the Ozone Ratis client. These properties are generated from the `RatisClientConfig` and its nested `RaftConfig` class. The property names, default values, and descriptions are taken directly from the source code.

| Property Name | Default Value                                                                       | Description |
|--------------|-------------------------------------------------------------------------------------|-------------|
| hdds.ratis.client.request.watch.type | ALL_COMMITTED                                                                       | Desired replication level when Ozone client's Raft client calls watch(), ALL_COMMITTED or MAJORITY_COMMITTED. MAJORITY_COMMITTED increases write performance by reducing watch() latency when an Ozone datanode is slow in a pipeline, at the cost of potential read latency increasing due to read retries to different datanodes. |
| hdds.ratis.client.request.write.timeout | 5m                                                                                  | Timeout for ratis client write request. |
| hdds.ratis.client.request.watch.timeout | 3m                                                                                  | Timeout for ratis client watch request. |
| hdds.ratis.client.multilinear.random.retry.policy | 5s, 5, 10s, 5, 15s, 5, 20s, 5, 25s, 5, 60s, 10                                      | Specifies multilinear random retry policy to be used by ratis client. e.g. given pairs of number of retries and sleep time (n0, t0), (n1, t1), ..., for the first n0 retries sleep duration is t0 on average, the following n1 retries sleep duration is t1 on average, and so on. |
| hdds.ratis.client.exponential.backoff.base.sleep | 4s                                                                                  | Specifies base sleep for exponential backoff retry policy. With the default base sleep of 4s, the sleep duration for ith retry is min(4 * pow(2, i), max_sleep) * r, where r is random number in the range [0.5, 1.5). |
| hdds.ratis.client.exponential.backoff.max.sleep | 40s                                                                                 | The sleep duration obtained from exponential backoff policy is limited by the configured max sleep. Refer hdds.ratis.client.exponential.backoff.base.sleep for further details. |
| hdds.ratis.client.exponential.backoff.max.retries | 2147483647                                                                          | Client's max retry value for the exponential backoff policy. |
| hdds.ratis.client.retrylimited.retry.interval | 1s                                                                                  | Interval between successive retries for a ratis client request. |
| hdds.ratis.client.retrylimited.max.retries | 180                                                                                 | Number of retries for ratis client request. |
| hdds.ratis.client.retry.policy | org.apache.hadoop.hdds.ratis.retrypolicy.<br>RequestTypeDependentRetryPolicyCreator | The class name of the policy for retry. |

### Raft Client Properties

The following properties are defined in the nested `RaftConfig` class inside `RatisClientConfig`.

| Property Name | Default Value | Description |
|--------------|--------------|-------------|
| hdds.ratis.client.raft.client.async.outstanding-requests.max | 32 | Controls the maximum number of outstanding async requests that can be handled by the Standalone as well as Ratis client. |
| hdds.ratis.client.raft.client.rpc.request.timeout | 60s | The timeout duration for ratis client request (except for watch request). It should be set greater than leader election timeout in Ratis. |
| hdds.ratis.client.raft.client.rpc.watch.request.timeout | 180s | The timeout duration for ratis client watch request. Timeout for the watch API in Ratis client to acknowledge a particular request getting replayed to all servers. It is highly recommended for the timeout duration to be strictly longer than Ratis server watch timeout (hdds.ratis.raft.server.watch.timeout). |
