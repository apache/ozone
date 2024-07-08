| **Name**        | `fs.trash.classname` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.ozone.om.TrashPolicyOzone |
| **Tag**         | OZONE, OZONEFS, CLIENT |
| **Description** | Trash Policy to be used. |
--------------------------------------------------------------------------------
| **Name**        | `hadoop.http.idle_timeout.ms` |
|:----------------|:----------------------------|
| **Value**       | 60000 |
| **Tag**         | OZONE, PERFORMANCE, S3GATEWAY |
| **Description** | OM/SCM/DN/S3GATEWAY Server connection timeout in milliseconds. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.block.token.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, HDDS, SECURITY, TOKEN |
| **Description** | True if block tokens are enabled, else false. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.block.token.expiry.time` |
|:----------------|:----------------------------|
| **Value**       | 1d |
| **Tag**         | OZONE, HDDS, SECURITY, TOKEN |
| **Description** | Default value for expiry time of block token. This setting supports multiple time unit suffixes as described in dfs.heartbeat.interval. If no suffix is specified, then milliseconds is assumed. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.command.status.report.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, DATANODE, MANAGEMENT |
| **Description** | Time interval of the datanode to send status of commands executed since last report. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.action.max.limit` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | DATANODE |
| **Description** | Maximum number of Container Actions sent by the datanode to SCM in a single heartbeat. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.checksum.verification.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, DATANODE |
| **Description** | To enable/disable checksum verification of the containers. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.chunk.write.sync` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, CONTAINER, MANAGEMENT |
| **Description** | Determines whether the chunk writes in the container happen as sync I/0 or buffered I/O operation. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.close.threshold` |
|:----------------|:----------------------------|
| **Value**       | 0.9f |
| **Tag**         | OZONE, DATANODE |
| **Description** | This determines the threshold to be used for closing a container. When the container used percentage reaches this threshold, the container will be closed. Value should be a positive, non-zero percentage in float notation (X.Yf), with 1.0f meaning 100%. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ipc.port` |
|:----------------|:----------------------------|
| **Value**       | 9859 |
| **Tag**         | OZONE, CONTAINER, MANAGEMENT |
| **Description** | The ipc port number of container. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ipc.random.port` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, DEBUG, CONTAINER |
| **Description** | Allocates a random free port for ozone container. This is used only while running unit tests. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.admin.port` |
|:----------------|:----------------------------|
| **Value**       | 9857 |
| **Tag**         | OZONE, CONTAINER, PIPELINE, RATIS, MANAGEMENT |
| **Description** | The ipc port number of container for admin requests. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.datanode.storage.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT, RATIS |
| **Description** | This directory is used for storing Ratis metadata like logs. If this is not set then default metadata dirs is used. A warning will be logged if this not set. Ideally, this should be mapped to a fast disk like an SSD. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.datastream.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, CONTAINER, RATIS, DATASTREAM |
| **Description** | It specifies whether to enable data stream of container. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.datastream.port` |
|:----------------|:----------------------------|
| **Value**       | 9855 |
| **Tag**         | OZONE, CONTAINER, RATIS, DATASTREAM |
| **Description** | The datastream port number of container. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.datastream.random.port` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, CONTAINER, RATIS, DATASTREAM |
| **Description** | Allocates a random free port for ozone container datastream. This is used only while running unit tests. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, MANAGEMENT, PIPELINE, RATIS |
| **Description** | Ozone supports different kinds of replication pipelines. Ratis is one of the replication pipeline supported by ozone. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.ipc.port` |
|:----------------|:----------------------------|
| **Value**       | 9858 |
| **Tag**         | OZONE, CONTAINER, PIPELINE, RATIS |
| **Description** | The ipc port number of container for clients. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.ipc.random.port
` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE,DEBUG |
| **Description** | Allocates a random free port for ozone ratis port for the container. This is used only while running unit tests. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.leader.pending.bytes.limit
` |
|:----------------|:----------------------------|
| **Value**       | 1GB |
| **Tag**         | OZONE, RATIS, PERFORMANCE |
| **Description** | Limit on the total bytes of pending requests after which leader starts rejecting requests from client. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.log.appender.queue.byte-limit
` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Byte limit for ratis leader's log appender queue. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.log.appender.queue.num-elements` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Limit for number of append entries in ratis leader's log appender queue. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.log.purge.gap
` |
|:----------------|:----------------------------|
| **Value**       | 1000000 |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Purge gap between the last purged commit index and the current index, when the leader decides to purge its log. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.log.queue.byte-limit` |
|:----------------|:----------------------------|
| **Value**       | 4GB |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Byte limit for Ratis Log Worker queue. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.log.queue.num-elements` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Limit for the number of operations in Ratis Log Worker. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.num.container.op.executors` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, RATIS, PERFORMANCE |
| **Description** | Number of executors that will be used by Ratis to execute container ops.(10 by default). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.num.write.chunk.threads.per.volume` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, RATIS, PERFORMANCE |
| **Description** | Maximum number of threads in the thread pool that Datanode will use for writing replicated chunks. This is a per configured locations! (10 thread per disk by default). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.replication.level` |
|:----------------|:----------------------------|
| **Value**       | MAJORITY |
| **Tag**         | OZONE, RATIS |
| **Description** | Replication level to be used by datanode for submitting a container command to ratis. Available replication levels are ALL and MAJORTIY, MAJORITY is used as the default replication level. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.rpc.type` |
|:----------------|:----------------------------|
| **Value**       | GRPC |
| **Tag**         | OZONE, RATIS, MANAGEMENT |
| **Description** | Ratis supports different kinds of transports like netty, GRPC, Hadoop RPC etc. This picks one of those for this cluster. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.segment.preallocated.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | OZONE, RATIS, PERFORMANCE |
| **Description** | The pre-allocated file size for raft segment used by Apache Ratis on datanodes. (4 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.segment.size` |
|:----------------|:----------------------------|
| **Value**       | 64MB |
| **Tag**         | OZONE, RATIS, PERFORMANCE |
| **Description** | The size of the raft segment file used by Apache Ratis on datanodes. (64 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.server.port` |
|:----------------|:----------------------------|
| **Value**       | 9856 |
| **Tag**         | OZONE, CONTAINER, PIPELINE, RATIS, MANAGEMENT |
| **Description** | The ipc port number of container for server-server communication. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.statemachine.max.pending.apply-transactions` |
|:----------------|:----------------------------|
| **Value**       | 10000 |
| **Tag**         | OZONE, RATIS |
| **Description** | Maximum number of pending apply transactions in a data pipeline. The default value is kept same as default snapshot threshold hdds.ratis.snapshot.threshold. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.statemachinedata.sync.retries` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Number of times the WriteStateMachineData op will be tried before failing. If the value is not configured, it will default to (hdds.ratis.rpc.slowness.timeout / hdds.container.ratis.statemachinedata.sync.timeout), which means that the WriteStatMachineData will be retried for every sync timeout until the configured slowness timeout is hit, after which the StateMachine will close down the pipeline. If this value is set to -1, then this retries indefinitely. This might not be desirable since if due to persistent failure the WriteStateMachineData op was not able to complete for a long time, this might block the Ratis write pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.ratis.statemachinedata.sync.timeout` |
|:----------------|:----------------------------|
| **Value**       | 10s |
| **Tag**         | OZONE, DEBUG, CONTAINER, RATIS |
| **Description** | Timeout for StateMachine data writes by Ratis. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.replication.compression` |
|:----------------|:----------------------------|
| **Value**       | NO_COMPRESSION |
| **Tag**         | OZONE, HDDS, DATANODE |
| **Description** | Compression algorithm used for closed container replication. Possible chooices include NO_COMPRESSION, GZIP, SNAPPY, LZ4, ZSTD |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.report.interval` |
|:----------------|:----------------------------|
| **Value**       | 60m |
| **Tag**         | OZONE, CONTAINER, MANAGEMENT |
| **Description** | Time interval of the datanode to send container report. Each datanode periodically send container report to SCM. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.container.token.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, HDDS, SECURITY, TOKEN |
| **Description** | True if container tokens are enabled, else false. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.client.address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, HDDS, MANAGEMENT |
| **Description** | The address of the Ozone Datanode client service. It is a string in the host:port format. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.client.bind.host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, HDDS, MANAGEMENT |
| **Description** | The hostname or IP address used by the Datanode client service endpoint to bind. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.client.port` |
|:----------------|:----------------------------|
| **Value**       | 19864 |
| **Tag**         | OZONE, HDDS, MANAGEMENT |
| **Description** | The port number of the Ozone Datanode client service. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.container.db.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | Determines where the per-disk rocksdb instances will be stored. This setting is optional. If unspecified, then rocksdb instances are stored on the same disk as HDDS data. The directories should be tagged with corresponding storage types ([SSD]/[DISK]/[ARCHIVE]/[RAM_DISK]) for storage policies. The default storage type will be DISK if the directory does not have a storage type tagged explicitly. Ideally, this should be mapped to a fast disk like an SSD. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | Determines where on the local filesystem HDDS data will be stored. Defaults to dfs.datanode.data.dir if not specified. The directories should be tagged with corresponding storage types ([SSD]/[DISK]/[ARCHIVE]/[RAM_DISK]) for storage policies. The default storage type will be DISK if the directory does not have a storage type tagged explicitly. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.dir.du.reserved` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | Reserved space in bytes per volume. Always leave this much space free for non dfs use. Such as /dir1:100B, /dir2:200MB, means dir1 reserves 100 bytes and dir2 reserves 200 MB. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.dir.du.reserved.percent` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | Percentage of volume that should be reserved. This space is left free for other usage. The value should be between 0-1. Such as 0.1 which means 10% of volume space will be reserved. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.handler.count` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | OZONE, HDDS, MANAGEMENT |
| **Description** | The number of RPC handler threads for Datanode client service endpoints. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9882 |
| **Tag**         | HDDS, MANAGEMENT |
| **Description** | The address and the base port where the Datanode web ui will listen on. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | HDDS, MANAGEMENT |
| **Description** | The actual address the Datanode web server will bind to. If this optional address is set, it overrides only the hostname portion of hdds.datanode.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | HDDS, SECURITY, MANAGEMENT, KERBEROS |
| **Description** | The kerberos keytab file for datanode http server |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | HDDS, SECURITY, MANAGEMENT, KERBEROS |
| **Description** | The kerberos principal for the datanode http server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | DATANODE, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | HDDS, MANAGEMENT |
| **Description** | Property to enable or disable Datanode web ui. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.https-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9883 |
| **Tag**         | HDDS, MANAGEMENT, SECURITY |
| **Description** | The address and the base port where the Datanode web UI will listen on using HTTPS. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | HDDS, MANAGEMENT, SECURITY |
| **Description** | The actual address the Datanode web server will bind to using HTTPS. If this optional address is set, it overrides only the hostname portion of hdds.datanode.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.metadata.rocksdb.cache.size` |
|:----------------|:----------------------------|
| **Value**       | 64MB |
| **Tag**         | OZONE, DATANODE, MANAGEMENT |
| **Description** | Size of the block metadata cache shared among RocksDB instances on each datanode. All containers on a datanode will share this cache. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.plugins` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Comma-separated list of HDDS datanode plug-ins to be activated when HDDS service starts as part of datanode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.ratis.server.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 2m |
| **Tag**         | OZONE, DATANODE |
| **Description** | Timeout for the request submitted directly to Ratis in datanode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.replication.work.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | DATANODE |
| **Description** | This configuration is deprecated. Temporary sub directory under each hdds.datanode.dir will be used during the container replication between datanodes to save the downloaded container(in compressed format). |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.slow.op.warning.threshold` |
|:----------------|:----------------------------|
| **Value**       | 500ms |
| **Tag**         | OZONE, DATANODE, PERFORMANCE |
| **Description** | Thresholds for printing slow-operation audit logs. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.storage.utilization.critical.threshold` |
|:----------------|:----------------------------|
| **Value**       | 0.95 |
| **Tag**         | OZONE, SCM, MANAGEMENT |
| **Description** | If a datanode overall storage utilization exceeds more than this value, the datanode will be marked out of space. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.storage.utilization.warning.threshold` |
|:----------------|:----------------------------|
| **Value**       | 0.75 |
| **Tag**         | OZONE, SCM, MANAGEMENT |
| **Description** | If a datanode overall storage utilization exceeds more than this value, a warning will be logged while processing the nodeReport in SCM. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.volume.choosing.policy` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | The class name of the policy for choosing volumes in the list of directories. Defaults to org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy. This volume choosing policy selects volumes in a round-robin order. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.datanode.volume.min.free.space` |
|:----------------|:----------------------------|
| **Value**       | 5GB |
| **Tag**         | OZONE, CONTAINER, STORAGE, MANAGEMENT |
| **Description** | This determines the free space to be used for closing containers When the difference between volume capacity and used reaches this number, containers that reside on this volume will be closed and no new containers would be allocated on this volume. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.db.profile` |
|:----------------|:----------------------------|
| **Value**       | DISK |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | This property allows user to pick a configuration that tunes the RocksDB settings for the hardware it is running on. Right now, we have SSD and DISK as profile options. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.grpc.tls.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, HDDS, SECURITY, TLS |
| **Description** | If HDDS GRPC server TLS is enabled. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.grpc.tls.provider` |
|:----------------|:----------------------------|
| **Value**       | OPENSSL |
| **Tag**         | OZONE, HDDS, SECURITY, TLS, CRYPTO_COMPLIANCE |
| **Description** | HDDS GRPC server TLS provider. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.heartbeat.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The heartbeat interval from a data node to SCM. Yes, it is not three but 30, since most data nodes will heart beating via Ratis heartbeats. If a client is not able to talk to a data node, it will notify OM/SCM eventually. So a 30 second HB seems to work. This assumes that replication strategy used is Ratis if not, this value should be set to something smaller like 3 seconds. ozone.scm.pipeline.close.timeout should also be adjusted accordingly, if the default value for this config is not used. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.key.algo` |
|:----------------|:----------------------------|
| **Value**       | RSA |
| **Tag**         | SCM, HDDS, X509, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | SCM CA key algorithm. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.key.dir.name` |
|:----------------|:----------------------------|
| **Value**       | keys |
| **Tag**         | SCM, HDDS, X509, SECURITY |
| **Description** | Directory to store public/private key for SCM CA. This is relative to ozone/hdds meteadata dir. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.key.len` |
|:----------------|:----------------------------|
| **Value**       | 2048 |
| **Tag**         | SCM, HDDS, X509, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | SCM CA key length. This is an algorithm-specific metric, such as modulus length, specified in number of bits. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.metadata.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | X509, SECURITY |
| **Description** | Absolute path to HDDS metadata dir. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.node.report.interval` |
|:----------------|:----------------------------|
| **Value**       | 60000ms |
| **Tag**         | OZONE, CONTAINER, MANAGEMENT |
| **Description** | Time interval of the datanode to send node report. Each datanode periodically send node report to SCM. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.pipeline.action.max.limit` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | DATANODE |
| **Description** | Maximum number of Pipeline Actions sent by the datanode to SCM in a single heartbeat. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.pipeline.report.interval` |
|:----------------|:----------------------------|
| **Value**       | 60000ms |
| **Tag**         | OZONE, PIPELINE, MANAGEMENT |
| **Description** | Time interval of the datanode to send pipeline report. Each datanode periodically send pipeline report to SCM. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `hdds.priv.key.file.name` |
|:----------------|:----------------------------|
| **Value**       | private.pem |
| **Tag**         | X509, SECURITY |
| **Description** | Name of file which stores private key generated for SCM CA. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.profiler.endpoint.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Enable /prof java profiler servlet page on HTTP server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.prometheus.endpoint.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Enable prometheus compatible metric page on the HTTP servers. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.public.key.file.name` |
|:----------------|:----------------------------|
| **Value**       | public.pem |
| **Tag**         | X509, SECURITY |
| **Description** | Name of file which stores public key generated for SCM CA. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.leader.election.minimum.timeout.duration` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, RATIS, MANAGEMENT |
| **Description** | The minimum timeout duration for ratis leader election. Default is 5s. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.server.retry-cache.timeout.duration` |
|:----------------|:----------------------------|
| **Value**       | 600000ms |
| **Tag**         | OZONE, RATIS, MANAGEMENT |
| **Description** | Retry Cache entry timeout for ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.ratis.snapshot.threshold` |
|:----------------|:----------------------------|
| **Value**       | 10000 |
| **Tag**         | OZONE, RATIS |
| **Description** | Number of transactions after which a ratis snapshot should be taken. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.recon.heartbeat.interval` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | OZONE, MANAGEMENT, RECON |
| **Description** | The heartbeat interval from a Datanode to Recon. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.rest.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9880 |
| **Tag**         |  |
| **Description** | The http address of Object Store REST server inside the datanode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | SCM, SECURITY, KERBEROS |
| **Description** | The keytab file used by SCM http server to login as its service principal if SPNEGO is enabled for SCM http server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | SCM, SECURITY, KERBEROS |
| **Description** | SCM http server service principal if SPNEGO is enabled for SCM http server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | OM, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.kerberos.keytab.file` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/SCM.keytab |
| **Tag**         | SCM, SECURITY, KERBEROS |
| **Description** | The keytab file used by SCM daemon to login as its service principal. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | SCM/_HOST@REALM |
| **Tag**         | SCM, SECURITY, KERBEROS |
| **Description** | The SCM service principal. e.g. scm/_HOST@REALM.COM |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.atleast.one.node.reported.pipeline.pct` |
|:----------------|:----------------------------|
| **Value**       | 0.90 |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Percentage of pipelines, where at least one datanode is reported in the pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Boolean value to enable or disable SCM safe mode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.healthy.pipeline.pct` |
|:----------------|:----------------------------|
| **Value**       | 0.10 |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Percentage of healthy pipelines, where all 3 datanodes are reported in the pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.min.datanode` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Minimum DataNodes which should be registered to get SCM out of safe mode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.pipeline-availability.check` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Boolean value to enable pipeline availability check during SCM safe mode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.pipeline.creation` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | Boolean value to enable background pipeline creation in SCM safe mode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.safemode.threshold.pct` |
|:----------------|:----------------------------|
| **Value**       | 0.99 |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | % of containers which should have at least one reported replica before SCM comes out of safe mode. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.scm.wait.time.after.safemode.exit` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | HDDS,SCM,OPERATION |
| **Description** | After exiting safemode, wait for configured interval of time to start replication monitor and cleanup activities of unhealthy pipelines. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.secret.key.algorithm` |
|:----------------|:----------------------------|
| **Value**       | HmacSHA256 |
| **Tag**         | SCM, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | The algorithm that SCM uses to generate symmetric secret keys. A valid algorithm is the one supported by KeyGenerator, as described at https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#KeyGenerator. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.secret.key.expiry.duration` |
|:----------------|:----------------------------|
| **Value**       | 7d |
| **Tag**         | SCM, SECURITY |
| **Description** | The duration for which symmetric secret keys issued by SCM are valid. This default value, in combination with hdds.secret.key.rotate.duration=1d, results in 7 secret keys (for the last 7 days) are kept valid at any point of time. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.secret.key.file.name` |
|:----------------|:----------------------------|
| **Value**       | secret_keys.json |
| **Tag**         | SCM, SECURITY |
| **Description** | Name of file which stores symmetric secret keys for token signatures. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.secret.key.rotate.check.duration` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | SCM, SECURITY |
| **Description** | The duration that SCM periodically checks if it's time to generate new symmetric secret keys. This config has an impact on the practical correctness of secret key expiry and rotation period. For example, if hdds.secret.key.rotate.duration=1d and hdds.secret.key.rotate.check.duration=10m, the actual key rotation will happen each 1d +/- 10m. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.secret.key.rotate.duration` |
|:----------------|:----------------------------|
| **Value**       | 1d |
| **Tag**         | SCM, SECURITY |
| **Description** | The duration that SCM periodically generate a new symmetric secret keys. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.datanode.container.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client datanode container protocol. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.block.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm block protocol. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.certificate.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm certificate protocol. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.container.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm container protocol. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.secretkey.datanode.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm secret key protocol for datanodes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.secretkey.om.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm secret key protocol for om. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.client.scm.secretkey.scm.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client scm secret key protocol for om. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.security.provider` |
|:----------------|:----------------------------|
| **Value**       | BC |
| **Tag**         | OZONE, HDDS, X509, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | The main security provider used for various cryptographic algorithms. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.tracing.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, HDDS |
| **Description** | If enabled, tracing information is sent to tracing server. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.ca.rotation.ack.timeout` |
|:----------------|:----------------------------|
| **Value**       | PT15M |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Max time that SCM leader will wait for the rotation preparation acks before it believes the rotation is failed. Default is 15 minutes. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.ca.rotation.check.interval` |
|:----------------|:----------------------------|
| **Value**       | P1D |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Check interval of whether internal root certificate is going to expire and needs to start rotation or not. Default is 1 day. The property value should be less than the value of property hdds.x509.renew.grace.duration. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.ca.rotation.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Whether auto root CA and sub CA certificate rotation is enabled or not. Default is disabled. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.ca.rotation.time-of-day` |
|:----------------|:----------------------------|
| **Value**       | 02:00:00 |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Time of day to start the rotation. Default 02:00 AM to avoid impacting daily workload. The supported format is 'hh:mm:ss', representing hour, minute, and second. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.default.duration` |
|:----------------|:----------------------------|
| **Value**       | P365D |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Default duration for which x509 certificates issued by SCM are valid. The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.dir.name` |
|:----------------|:----------------------------|
| **Value**       | certs |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | X509 certificate directory name. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.expired.certificate.check.interval` |
|:----------------|:----------------------------|
| **Value**       | P1D |
| **Tag**         |  |
| **Description** | Interval to use for removing expired certificates. A background task to remove expired certificates from the scm metadata store is scheduled to run at the rate this configuration option specifies. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.file.name` |
|:----------------|:----------------------------|
| **Value**       | certificate.crt |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Certificate file name. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.max.duration` |
|:----------------|:----------------------------|
| **Value**       | P1865D |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Max time for which certificate issued by SCM CA are valid. This duration is used for self-signed root cert and scm sub-ca certs issued by root ca. The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.renew.grace.duration` |
|:----------------|:----------------------------|
| **Value**       | P28D |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Duration of the grace period within which a certificate should be * renewed before the current one expires. Default is 28 days. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.rootca.certificate.file` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Path to an external CA certificate. The file format is expected to be pem. This certificate is used when initializing SCM to create a root certificate authority. By default, a self-signed certificate is generated instead. Note that this certificate is only used for Ozone's internal communication, and it does not affect the certificates used for HTTPS protocol at WebUIs as they can be configured separately. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.rootca.certificate.polling.interval` |
|:----------------|:----------------------------|
| **Value**       | PT2h |
| **Tag**         |  |
| **Description** | Interval to use for polling in certificate clients for a new root ca certificate. Every time the specified time duration elapses, the clients send a request to the SCMs to see if a new root ca certificate was generated. Once there is a change, the system automatically adds the new root ca to the clients' trust stores and requests a new certificate to be signed. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.rootca.private.key.file` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Path to an external private key. The file format is expected to be pem. This private key is later used when initializing SCM to sign certificates as the root certificate authority. When not specified a private and public key is generated instead. These keys are only used for Ozone's internal communication, and it does not affect the HTTPS protocol at WebUIs as they can be configured separately. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.rootca.public.key.file` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Path to an external public key. The file format is expected to be pem. This public key is later used when initializing SCM to sign certificates as the root certificate authority. When only the private key is specified the public key is read from the external certificate. Note that this is only used for Ozone's internal communication, and it does not affect the HTTPS protocol at WebUIs as they can be configured separately. |
--------------------------------------------------------------------------------
| **Name**        | `hdds.x509.signature.algorithm` |
|:----------------|:----------------------------|
| **Value**       | SHA256withRSA |
| **Tag**         | OZONE, HDDS, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | X509 signature certificate. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.UnsafeByteOperations.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, PERFORMANCE, CLIENT |
| **Description** | It specifies whether to use unsafe or safe buffer to byteString copy. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.acl.authorizer.class` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer |
| **Tag**         | OZONE, SECURITY, ACL |
| **Description** | Acl authorizer for Ozone. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.acl.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SECURITY, ACL |
| **Description** | Key to enable/disable ozone acls. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.administrators` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY |
| **Description** | Ozone administrator users delimited by the comma. If not set, only the user who launches an ozone service will be the admin user. This property must be set if ozone services are started by different users. Otherwise, the RPC layer will reject calls from other servers which are started by users not in the list. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.administrators.groups` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY |
| **Description** | Ozone administrator groups delimited by the comma. This is the list of groups who can access admin only information from ozone. It is enough to either have the name defined in ozone.administrators or be directly or indirectly in a group defined in this property. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.audit.log.debug.cmd.list.dnaudit` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | DATANODE |
| **Description** | A comma separated list of Datanode commands that are written to the DN audit logs only if the audit log level is debug. Ex: "CREATE_CONTAINER,READ_CONTAINER,UPDATE_CONTAINER". |
--------------------------------------------------------------------------------
| **Name**        | `ozone.audit.log.debug.cmd.list.omaudit` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM |
| **Description** | A comma separated list of OzoneManager commands that are written to the OzoneManager audit logs only if the audit log level is debug. Ex: "ALLOCATE_BLOCK,ALLOCATE_KEY,COMMIT_KEY". |
--------------------------------------------------------------------------------
| **Name**        | `ozone.audit.log.debug.cmd.list.scmaudit` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | SCM |
| **Description** | A comma separated list of SCM commands that are written to the SCM audit logs only if the audit log level is debug. Ex: "GET_VERSION,REGISTER,SEND_HEARTBEAT". |
--------------------------------------------------------------------------------
| **Name**        | `ozone.block.deleting.container.limit.per.interval` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, PERFORMANCE, SCM |
| **Description** | A maximum number of containers to be scanned by block deleting service per time interval. The block deleting service spawns a thread to handle block deletions in a container. This property is used to throttle the number of threads spawned for block deletions. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.block.deleting.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, PERFORMANCE, SCM |
| **Description** | A maximum number of blocks to be deleted by block deleting service per time interval. This property is used to throttle the actual number of block deletions on a data node per container. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.block.deleting.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, PERFORMANCE, SCM |
| **Description** | Time interval of the block deleting service. The block deleting service runs on each datanode periodically and deletes blocks queued for deletion. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.block.deleting.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300000ms |
| **Tag**         | OZONE, PERFORMANCE, SCM |
| **Description** | A timeout value of block deletion service. If this is set greater than 0, the service will stop waiting for the block deleting completion after this time. If timeout happens to a large proportion of block deletion, this needs to be increased with ozone.block.deleting.limit.per.task. This setting supports multiple time unit suffixes as described in dfs.heartbeat.interval. If no suffix is specified, then milliseconds is assumed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.block.deleting.service.workers` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, PERFORMANCE, SCM |
| **Description** | Number of workers executed of block deletion service. This configuration should be set to greater than 0. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.chunk.read.buffer.default.size` |
|:----------------|:----------------------------|
| **Value**       | 1MB |
| **Tag**         | OZONE, SCM, CONTAINER, PERFORMANCE |
| **Description** | The default read buffer size during read chunk operations when checksum is disabled. Chunk data will be cached in buffers of this capacity. For chunk data with checksum, the read buffer size will be the same as the number of bytes per checksum (ozone.client.bytes.per.checksum) corresponding to the chunk. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.chunk.read.mapped.buffer.threshold` |
|:----------------|:----------------------------|
| **Value**       | 32KB |
| **Tag**         | OZONE, SCM, CONTAINER, PERFORMANCE |
| **Description** | The default read threshold to use memory mapped buffers. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.bucket.replication.config.refresh.time.ms` |
|:----------------|:----------------------------|
| **Value**       | 30000 |
| **Tag**         | OZONE |
| **Description** | Default time period to refresh the bucket replication config in o3fs clients. Until the bucket replication config refreshed, client will continue to use existing replication config irrespective of whether bucket replication config updated at OM or not. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.connection.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5000ms |
| **Tag**         | OZONE, PERFORMANCE, CLIENT |
| **Description** | Connection timeout for Ozone client in milliseconds. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.grpc.retries.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | CLIENT |
| **Description** | To enable Grpc client retries for EC. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.grpc.retries.max` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | CLIENT |
| **Description** | The maximum attempts GRPC client makes before failover. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.ec.grpc.write.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, CLIENT, MANAGEMENT |
| **Description** | Timeout for ozone ec grpc client during write. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.failover.max.attempts` |
|:----------------|:----------------------------|
| **Value**       | 500 |
| **Tag**         |  |
| **Description** | Expert only. Ozone RpcClient attempts talking to each OzoneManager ipc.client.connect.max.retries (default = 10) number of times before failing over to another OzoneManager, if available. This parameter represents the number of times the client will failover before giving up. This value is kept high so that client does not give up trying to connect to OMs easily. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.fs.default.bucket.layout` |
|:----------------|:----------------------------|
| **Value**       | FILE_SYSTEM_OPTIMIZED |
| **Tag**         | OZONE, CLIENT |
| **Description** | Default bucket layout value used when buckets are created using OFS. Supported values are LEGACY and FILE_SYSTEM_OPTIMIZED. FILE_SYSTEM_OPTIMIZED: This layout allows the bucket to support atomic rename/delete operations and also allows interoperability between S3 and FS APIs. Keys written via S3 API with a "/" delimiter will create intermediate directories. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.key.latest.version.location` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, CLIENT |
| **Description** | Ozone client gets the latest version location. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.key.provider.cache.expiry` |
|:----------------|:----------------------------|
| **Value**       | 10d |
| **Tag**         | OZONE, CLIENT, SECURITY |
| **Description** | Ozone client security key provider cache expiration time. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.list.cache` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, PERFORMANCE |
| **Description** | Configuration property to configure the cache size of client list calls. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.list.trash.keys.max` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, CLIENT |
| **Description** | The maximum number of keys to return for a list trash request. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.max.ec.stripe.write.retries` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | CLIENT |
| **Description** | When EC stripe write failed, client will request to allocate new block group and write the failed stripe into new block group. If the same stripe failure continued in newly acquired block group also, then it will retry by requesting to allocate new block group again. This configuration is used to limit these number of retries. By default the number of retries are 10. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.read.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, CLIENT, MANAGEMENT |
| **Description** | Timeout for ozone grpc client during read. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.socket.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5000ms |
| **Tag**         | OZONE, CLIENT |
| **Description** | Socket timeout for Ozone client. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.client.wait.between.retries.millis` |
|:----------------|:----------------------------|
| **Value**       | 2000 |
| **Tag**         |  |
| **Description** | Expert only. The time to wait, in milliseconds, between retry attempts to contact OM. Wait time increases linearly if same OM is retried again. If retrying on multiple OMs proxies in round robin fashion, the wait time is introduced after all the OM proxies have been attempted once. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.container.cache.lock.stripes` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | PERFORMANCE, CONTAINER, STORAGE |
| **Description** | Container DB open is an exclusive operation. We use a stripe lock to guarantee that different threads can open different container DBs concurrently, while for one container DB, only one thread can open it at the same time. This setting controls the lock stripes. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.container.cache.size` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | PERFORMANCE, CONTAINER, STORAGE |
| **Description** | The open container is cached on the data node side. We maintain an LRU cache for caching the recently used containers. This setting controls the size of that cache. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.default.bucket.layout` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Default bucket layout used by Ozone Manager during bucket creation when a client does not specify the bucket layout option. Supported values are OBJECT_STORE and FILE_SYSTEM_OPTIMIZED. OBJECT_STORE: This layout allows the bucket to behave as a pure object store and will not allow interoperability between S3 and FS APIs. FILE_SYSTEM_OPTIMIZED: This layout allows the bucket to support atomic rename/delete operations and also allows interoperability between S3 and FS APIs. Keys written via S3 API with a "/" delimiter will create intermediate directories. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.directory.deleting.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | Time interval of the directory deleting service. It runs on OM periodically and cleanup orphan directory and its sub-tree. For every orphan directory it deletes the sub-path tree structure(dirs/files). It sends sub-files to KeyDeletingService to deletes its blocks. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.ec.grpc.zerocopy.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, DATANODE |
| **Description** | Specify if zero-copy should be enabled for EC GRPC protocol. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.filesystem.snapshot.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM |
| **Description** | Enables Ozone filesystem snapshot feature if set to true on the OM side. Disables it otherwise. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9884 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The address and the base port where the FREON web ui will listen on. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The actual address the Freon web server will bind to. If this optional address is set, it overrides only the hostname portion of ozone.freon.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | SECURITY |
| **Description** | Keytab used by Freon. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | SECURITY |
| **Description** | Security principal used by freon. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | FREON, SECURITY |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Property to enable or disable FREON web ui. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.https-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9885 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The address and the base port where the Freon web server will listen on using HTTPS. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.freon.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The actual address the Freon web server will bind to using HTTPS. If this optional address is set, it overrides only the hostname portion of ozone.freon.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.datastream.auto.threshold` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | OZONE, DATANODE |
| **Description** | A threshold to auto select datastream to write files in OzoneFileSystem. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.datastream.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, DATANODE |
| **Description** | To enable/disable filesystem write via ratis streaming. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.hsync.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, CLIENT |
| **Description** | Enable hsync/hflush. By default they are disabled. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.iterate.batch-size` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, OZONEFS |
| **Description** | Iterate batch size of delete when use BasicOzoneFileSystem. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.listing.page.size` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OZONE, CLIENT |
| **Description** | Listing page size value used by client for listing number of items on fs related sub-commands output. Kindly set this config value responsibly to avoid high resource usage. Maximum value restricted is 5000 for optimum performance. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.fs.listing.page.size.max` |
|:----------------|:----------------------------|
| **Value**       | 5000 |
| **Tag**         | OZONE, OM |
| **Description** | Maximum listing page size value enforced by server for listing items on fs related sub-commands output. Kindly set this config value responsibly to avoid high resource usage. Maximum value restricted is 5000 for optimum performance. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.http.basedir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM, SCM, MANAGEMENT |
| **Description** | The base dir for HTTP Jetty server to extract contents. If this property is not configured, by default, Jetty will create a directory inside the directory named by the ${ozone.metadata.dirs}/webserver. While in production environment, it's strongly suggested instructing Jetty to use a different parent directory by setting this property to the name of the desired parent directory. The value of the property will be used to set Jetty context attribute 'org.eclipse.jetty.webapp.basetempdir'. The directory named by this property must exist and be writeable. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.http.filter.initializers` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | Set to org.apache.hadoop.security.AuthenticationFilterInitializer to enable Kerberos authentication for Ozone HTTP web consoles is enabled using the SPNEGO protocol. When this property is set, ozone.security.http.kerberos.enabled should be set to true. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.http.policy` |
|:----------------|:----------------------------|
| **Value**       | HTTP_ONLY |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Decide if HTTPS(SSL) is supported on Ozone This configures the HTTP endpoint for Ozone daemons: The following values are supported: - HTTP_ONLY : Service is provided only on http - HTTPS_ONLY : Service is provided only on https - HTTP_AND_HTTPS : Service is provided both on http and https |
--------------------------------------------------------------------------------
| **Name**        | `ozone.https.client.keystore.resource` |
|:----------------|:----------------------------|
| **Value**       | ssl-client.xml |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Resource file from which ssl client keystore information will be extracted |
--------------------------------------------------------------------------------
| **Name**        | `ozone.https.client.need-auth` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Whether SSL client certificate authentication is required |
--------------------------------------------------------------------------------
| **Name**        | `ozone.https.server.keystore.resource` |
|:----------------|:----------------------------|
| **Value**       | ssl-server.xml |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Resource file from which ssl server keystore information will be extracted |
--------------------------------------------------------------------------------
| **Name**        | `ozone.key.deleting.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 20000 |
| **Tag**         | OM, PERFORMANCE |
| **Description** | A maximum number of keys to be scanned by key deleting service per time interval in OM. Those keys are sent to delete metadata and generate transactions in SCM for next async deletion between SCM and DataNode. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.key.preallocation.max.blocks` |
|:----------------|:----------------------------|
| **Value**       | 64 |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | While allocating blocks from OM, this configuration limits the maximum number of blocks being allocated. This configuration ensures that the allocated block response do not exceed rpc payload limit. If client needs more space for the write, separate block allocation requests will be made. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.manager.db.checkpoint.transfer.bandwidthPerSec` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | OZONE |
| **Description** | Maximum bandwidth used for Ozone Manager DB checkpoint download through the servlet. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.manager.delegation.remover.scan.interval` |
|:----------------|:----------------------------|
| **Value**       | 3600000 |
| **Tag**         |  |
| **Description** | Time interval after which ozone secret manger scans for expired delegation token. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.manager.delegation.token.max-lifetime` |
|:----------------|:----------------------------|
| **Value**       | 7d |
| **Tag**         |  |
| **Description** | Default max time interval after which ozone delegation token will not be renewed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.manager.delegation.token.renew-interval` |
|:----------------|:----------------------------|
| **Value**       | 1d |
| **Tag**         |  |
| **Description** | Default time interval after which ozone delegation token will require renewal before any further use. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.metadata.dirs` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM, SCM, CONTAINER, STORAGE, REQUIRED |
| **Description** | This setting is the fallback location for SCM, OM, Recon and DataNodes to store their metadata. This setting may be used only in test/PoC clusters to simplify configuration. For production clusters or any time you care about performance, it is recommended that ozone.om.db.dirs, ozone.scm.db.dirs and hdds.container.ratis.datanode.storage.dir be configured separately. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.metadata.dirs.permissions` |
|:----------------|:----------------------------|
| **Value**       | 750 |
| **Tag**         |  |
| **Description** | Permissions for the metadata directories for fallback location for SCM, OM, Recon and DataNodes to store their metadata. The permissions have to be octal or symbolic. This is the fallback used in case the default permissions for OM,SCM,Recon,Datanode are not set. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.metastore.rocksdb.cf.write.buffer.size` |
|:----------------|:----------------------------|
| **Value**       | 128MB |
| **Tag**         | OZONE, OM, SCM, STORAGE, PERFORMANCE |
| **Description** | The write buffer (memtable) size for each column family of the rocksdb store. Check the rocksdb documentation for more details. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.metastore.rocksdb.statistics` |
|:----------------|:----------------------------|
| **Value**       | OFF |
| **Tag**         | OZONE, OM, SCM, STORAGE, PERFORMANCE |
| **Description** | The statistics level of the rocksdb store. If you use any value from org.rocksdb.StatsLevel (eg. ALL or EXCEPT_DETAILED_TIMERS), the rocksdb statistics will be exposed over JMX bean with the choosed setting. Set it to OFF to not initialize rocksdb statistics at all. Please note that collection of statistics could have 5-10% performance penalty. Check the rocksdb documentation for more details. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.network.flexible.fqdn.resolution.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SCM, OM |
| **Description** | SCM, OM hosts will be able to resolve itself based on its host name instead of fqdn. It is useful for deploying to kubernetes environment, during the initial launching time when [pod_name].[service_name] is not resolvable yet because of the probe. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.network.jvm.address.cache.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, SCM, OM, DATANODE |
| **Description** | Disable the jvm network address cache. In environment such as kubernetes, IPs of instances of scm, om and datanodes can be changed. Disabling this cache helps to quickly resolve the fqdn's to the new IPs. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.network.topology.aware.read` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, PERFORMANCE |
| **Description** | Whether to enable topology aware read to improve the read performance. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9862 |
| **Tag**         | OM, REQUIRED |
| **Description** | The address of the Ozone OM service. This allows clients to discover the address of the OM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.admin.protocol.max.retries` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Expert only. The maximum number of retries for Ozone Manager Admin protocol on each OM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.admin.protocol.wait.between.retries` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Expert only. The time to wait, in milliseconds, between retry attempts for Ozone Manager Admin protocol. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.container.location.cache.size` |
|:----------------|:----------------------------|
| **Value**       | 100000 |
| **Tag**         | OZONE, OM |
| **Description** | The size of the container locations cache in Ozone Manager. This cache allows Ozone Manager to populate block locations in key-read responses without calling SCM, thus increases Ozone Manager read performance. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.container.location.cache.ttl` |
|:----------------|:----------------------------|
| **Value**       | 360m |
| **Tag**         | OZONE, OM |
| **Description** | The time to live for container location cache in Ozone. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.db.dirs` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM, STORAGE, PERFORMANCE |
| **Description** | Directory where the OzoneManager stores its metadata. This should be specified as a single directory. If the directory does not exist then the OM will attempt to create it. If undefined, then the OM will log a warning and fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.db.dirs.permissions` |
|:----------------|:----------------------------|
| **Value**       | 750 |
| **Tag**         |  |
| **Description** | Permissions for the metadata directories for Ozone Manager. The permissions have to be octal or symbolic. If the default permissions are not set then the default value of 750 will be used. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.decommissioned.nodes.EXAMPLEOMSERVICEID` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM, HA |
| **Description** | Comma-separated list of OM node Ids which have been decommissioned. OMs present in this list will not be included in the OM HA ring. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.delta.update.data.size.max.limit` |
|:----------------|:----------------------------|
| **Value**       | 1024MB |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Recon get a limited delta updates from OM periodically since sequence number. Based on sequence number passed, OM DB delta update may have large number of log files and each log batch data may be huge depending on frequent writes and updates by ozone client, so to avoid increase in heap memory, this config is used as limiting factor of default 1 GB while preparing DB updates object. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.enable.filesystem.paths` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | If true, key names will be interpreted as file system paths. "/" will be treated as a special character and paths will be normalized and must follow Unix filesystem path naming conventions. This flag will be helpful when objects created by S3G need to be accessed using OFS/O3Fs. If false, it will fallback to default behavior of Key/MPU create requests where key paths are not normalized and any intermediate directories will not be created or any file checks happens to check filesystem semantics. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.enable.ofs.shared.tmp.dir` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | Enable shared ofs tmp directory ofs://tmp. Allows a root tmp directory with sticky-bit behaviour. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.fs.snapshot.max.limit` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, OM, MANAGEMENT |
| **Description** | The maximum number of filesystem snapshot allowed in an Ozone Manager. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.grpc.bossgroup.size` |
|:----------------|:----------------------------|
| **Value**       | 8 |
| **Tag**         | OZONE, OM, S3GATEWAY |
| **Description** | OM grpc server netty boss event group size. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.grpc.maximum.response.length` |
|:----------------|:----------------------------|
| **Value**       | 134217728 |
| **Tag**         | OZONE, OM, S3GATEWAY |
| **Description** | OM/S3GATEWAY OMRequest, OMResponse over grpc max message length (bytes). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.grpc.read.thread.num` |
|:----------------|:----------------------------|
| **Value**       | 32 |
| **Tag**         | OZONE, OM, S3GATEWAY |
| **Description** | OM grpc server read thread pool core thread size. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.grpc.workergroup.size` |
|:----------------|:----------------------------|
| **Value**       | 32 |
| **Tag**         | OZONE, OM, S3GATEWAY |
| **Description** | OM grpc server netty worker event group size. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OM, PERFORMANCE |
| **Description** | The number of RPC handler threads for OM service endpoints. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9874 |
| **Tag**         | OM, MANAGEMENT |
| **Description** | The address and the base port where the OM web UI will listen on. If the port is 0, then the server will start on a free port. However, it is best to specify a well-known port, so it is easy to connect and see the OM management UI. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OM, MANAGEMENT |
| **Description** | The actual address the OM web server will bind to. If this optional the address is set, it overrides only the hostname portion of ozone.om.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | The keytab file used by OM http server to login as its service principal if SPNEGO is enabled for om http server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | Ozone Manager http server service principal if SPNEGO is enabled for om http server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | OM, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Property to enable or disable OM web user interface. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.https-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9875 |
| **Tag**         | OM, MANAGEMENT, SECURITY |
| **Description** | The address and the base port where the OM web UI will listen on using HTTPS. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OM, MANAGEMENT, SECURITY |
| **Description** | The actual address the OM web server will bind to using HTTPS. If this optional address is set, it overrides only the hostname portion of ozone.om.https-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.internal.service.id` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM, HA |
| **Description** | Service ID of the Ozone Manager. If this is not set fall back to ozone.om.service.ids to find the service ID it belongs to. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.kerberos.keytab.file` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/OM.keytab |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | The keytab file used by OzoneManager daemon to login as its service principal. The principal name is configured with ozone.om.kerberos.principal. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | OM/_HOST@REALM |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | The OzoneManager service principal. Ex om/_HOST@REALM.COM |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.kerberos.principal.pattern` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         |  |
| **Description** | A client-side RegEx that can be configured to control allowed realms to authenticate with (useful in cross-realm env.) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.key.path.lock.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | Defaults to false. If true, the fine-grained KEY_PATH_LOCK functionality is enabled. If false, it is disabled. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.keyname.character.check.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | If true, then enable to check if the key name contains illegal characters when creating/renaming key. For the definition of illegal characters, follow the rules in Amazon S3's object key naming guide. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.leader.election.minimum.timeout.duration` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT, DEPRECATED |
| **Description** | DEPRECATED. Leader election timeout uses ratis rpc timeout which can be set via ozone.om.ratis.minimum.timeout. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.lock.fair` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         |  |
| **Description** | If this is true, the Ozone Manager lock will be used in Fair mode, which will schedule threads in the order received/queued. If this is false, uses non-fair ordering. See java.util.concurrent.locks.ReentrantReadWriteLock for more information on fair/non-fair locks. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.max.buckets` |
|:----------------|:----------------------------|
| **Value**       | 100000 |
| **Tag**         | OZONE, OM |
| **Description** | maximum number of buckets across all volumes. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.multitenancy.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | Enable S3 Multi-Tenancy. If disabled, all S3 multi-tenancy requests are rejected. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.multitenancy.ranger.sync.interval` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | OZONE, OM |
| **Description** | Determines how often the Multi-Tenancy Ranger background sync thread service should run. Background thread periodically checks Ranger policies and roles created by Multi-Tenancy feature. And overwrites them if obvious discrepancies are detected. Value should be set with a unit suffix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.multitenancy.ranger.sync.timeout` |
|:----------------|:----------------------------|
| **Value**       | 10s |
| **Tag**         | OZONE, OM |
| **Description** | The timeout for each Multi-Tenancy Ranger background sync thread run. If the timeout has been reached, a warning message will be logged. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.namespace.s3.strict` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM |
| **Description** | Ozone namespace should follow S3 naming rule by default. However this parameter allows the namespace to support non-S3 compatible characters. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.network.topology.refresh.duration` |
|:----------------|:----------------------------|
| **Value**       | 1h |
| **Tag**         | SCM, OZONE, OM |
| **Description** | The duration at which we periodically fetch the updated network topology cluster tree from SCM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.node.id` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM, HA |
| **Description** | The ID of this OM node. If the OM node ID is not configured it is determined automatically by matching the local node's address with the configured address. If node ID is not deterministic from the configuration, then it is set to default node id - om1. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.nodes.EXAMPLEOMSERVICEID` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM, HA |
| **Description** | Comma-separated list of OM node Ids for a given OM service ID (eg. EXAMPLEOMSERVICEID). The OM service ID should be the value (one of the values if there are multiple) set for the parameter ozone.om.service.ids. Decommissioned nodes (represented by node Ids in ozone.om.decommissioned.nodes config list) will be ignored and not included in the OM HA setup even if added to this list. Unique identifiers for each OM Node, delimited by commas. This will be used by OzoneManagers in HA setup to determine all the OzoneManagers belonging to the same OMservice in the cluster. For example, if you used “omService1” as the OM service ID previously, and you wanted to use “om1”, “om2” and "om3" as the individual IDs of the OzoneManagers, you would configure a property ozone.om.nodes.omService1, and its value "om1,om2,om3". |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.key.cleanup.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | The maximum number of open keys to be identified as expired and marked for deletion by one run of the open key cleanup service on the OM. This property is used to throttle the actual number of open key deletions on the OM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.key.cleanup.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 24h |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | A background job that periodically checks open key entries and marks expired open keys for deletion. This entry controls the interval of this cleanup check. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.key.cleanup.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | A timeout value of open key cleanup service. If this is set greater than 0, the service will stop waiting for the open key deleting completion after this time. If timeout happens to a large proportion of open key deletion, this value needs to be increased or ozone.om.open.key.cleanup.limit.per.task should be decreased. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.key.expire.threshold` |
|:----------------|:----------------------------|
| **Value**       | 7d |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | Controls how long an open key operation is considered active. Specifically, if a key has been open longer than the value of this config entry, that open key is considered as expired (e.g. due to client crash). Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.mpu.cleanup.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 24h |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | A background job that periodically checks inactive multipart info send multipart upload abort requests for them. This entry controls the interval of this cleanup check. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.mpu.cleanup.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | A timeout value of multipart upload cleanup service. If this is set greater than 0, the service will stop waiting for the multipart info abort completion after this time. If timeout happens to a large proportion of multipart aborts, this value needs to be increased or ozone.om.open.key.cleanup.limit.per.task should be decreased. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.mpu.expire.threshold` |
|:----------------|:----------------------------|
| **Value**       | 30d |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | Controls how long multipart upload is considered active. Specifically, if a multipart info has been ongoing longer than the value of this config entry, that multipart info is considered as expired (e.g. due to client crash). Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.open.mpu.parts.cleanup.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, OM, PERFORMANCE |
| **Description** | The maximum number of parts, rounded up to the nearest number of expired multipart upload. This property is used to approximately throttle the number of MPU parts sent to the OM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.enable` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | Property to enable or disable Ratis server on OM. Please note - this is a temporary property to disable OM Ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.log.appender.queue.byte-limit` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | OZONE, DEBUG, OM, RATIS |
| **Description** | Byte limit for Raft's Log Worker queue. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.log.appender.queue.num-elements` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OZONE, DEBUG, OM, RATIS |
| **Description** | Number of operation pending with Raft's Log Worker. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.log.purge.gap` |
|:----------------|:----------------------------|
| **Value**       | 1000000 |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | The minimum gap between log indices for Raft server to purge its log segments after taking snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.log.purge.preservation.log.num` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | The number of latest Raft logs to not be purged after taking snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.log.purge.upto.snapshot.index` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | Enable/disable Raft server to purge its log up to the snapshot index after taking snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.minimum.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | The minimum timeout duration for OM's Ratis server rpc. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.port` |
|:----------------|:----------------------------|
| **Value**       | 9872 |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | The port number of the OzoneManager's Ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.rpc.type` |
|:----------------|:----------------------------|
| **Value**       | GRPC |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | Ratis supports different kinds of transports like netty, GRPC, Hadoop RPC etc. This picks one of those for this cluster. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.segment.preallocated.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | OZONE, OM, RATIS, PERFORMANCE |
| **Description** | The size of the buffer which is preallocated for raft segment used by Apache Ratis on OM.(4 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.segment.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | OZONE, OM, RATIS, PERFORMANCE |
| **Description** | The size of the raft segment used by Apache Ratis on OM. (4 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.server.close.threshold` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | Raft Server will close if JVM pause longer than the threshold. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.server.failure.timeout.duration` |
|:----------------|:----------------------------|
| **Value**       | 120s |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | The timeout duration for ratis server failure detection, once the threshold has reached, the ratis state machine will be informed about the failure in the ratis ring. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.server.leaderelection.pre-vote` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | Enable/disable OM HA leader election pre-vote phase. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.server.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 3s |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | The timeout duration for OM's ratis server request . |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.server.retry.cache.timeout` |
|:----------------|:----------------------------|
| **Value**       | 600000ms |
| **Tag**         | OZONE, OM, RATIS, MANAGEMENT |
| **Description** | Retry Cache entry timeout for OM's ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.snapshot.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM, STORAGE, MANAGEMENT, RATIS |
| **Description** | This directory is used for storing OM's snapshot related files like the ratisSnapshotIndex and DB checkpoint from leader OM. If undefined, OM snapshot dir will fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.snapshot.max.total.sst.size` |
|:----------------|:----------------------------|
| **Value**       | 100000000 |
| **Tag**         | OZONE, OM, RATIS |
| **Description** | Max size of SST files in OM Ratis Snapshot tarball. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.ratis.storage.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM, STORAGE, MANAGEMENT, RATIS |
| **Description** | This directory is used for storing OM's Ratis metadata like logs. If this is not set then default metadata dirs is used. A warning will be logged if this not set. Ideally, this should be mapped to a fast disk like an SSD. If undefined, OM ratis storage dir will fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.save.metrics.interval` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, OM |
| **Description** | Time interval used to store the omMetrics in to a file. Background thread periodically stores the OM metrics in to a file. Unit could be defined with postfix (ns,ms,s,m,h,d) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.security.admin.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access ozone manager admin protocol. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.security.client.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access client ozone manager protocol. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.service.ids` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OM, HA |
| **Description** | Comma-separated list of OM service Ids. This property allows the client to figure out quorum of OzoneManager address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.cache.cleanup.service.run.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, OM |
| **Description** | Interval at which snapshot cache clean up will run. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.cache.max.size` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, OM |
| **Description** | Size of the OM Snapshot LRU cache. This is a soft limit of open OM Snapshot RocksDB instances that will be held. The actual number of cached instance could exceed this limit if more than this number of snapshot instances are still in-use by snapDiff or other tasks. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.checkpoint.dir.creation.poll.timeout` |
|:----------------|:----------------------------|
| **Value**       | 20s |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | Max poll timeout for snapshot dir exists check performed before loading a snapshot in cache. Unit defaults to millisecond if a unit is not specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.compaction.dag.max.time.allowed` |
|:----------------|:----------------------------|
| **Value**       | 30d |
| **Tag**         | OZONE, OM |
| **Description** | Maximum time a snapshot is allowed to be in compaction DAG before it gets pruned out by pruning daemon. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.compaction.dag.prune.daemon.run.interval` |
|:----------------|:----------------------------|
| **Value**       | 3600s |
| **Tag**         | OZONE, OM |
| **Description** | Interval at which compaction DAG pruning daemon thread is running to remove older snapshots with compaction history from compaction DAG. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.db.max.open.files` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, OM |
| **Description** | Max number of open files for each snapshot db present in the snapshot cache. Essentially sets `max_open_files` config for RocksDB instances opened for Ozone snapshots. This will limit the total number of files opened by a snapshot db thereby limiting the total number of open file handles by snapshot dbs. Max total number of open handles = (snapshot cache size * max open files) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.cleanup.service.run.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, OM |
| **Description** | Interval at which snapshot diff clean up service will run. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.cleanup.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, OM |
| **Description** | Timeout for snapshot diff clean up service. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.db.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, OM |
| **Description** | Directory where the OzoneManager stores the snapshot diff related data. This should be specified as a single directory. If the directory does not exist then the OM will attempt to create it. If undefined, then the OM will log a warning and fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.disable.native.libs` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | Flag to perform snapshot diff without using native libs(can be slow). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.job.default.wait.time` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, OM |
| **Description** | Default wait time returned to client to wait before retrying snap diff request. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.job.report.persistent.time` |
|:----------------|:----------------------------|
| **Value**       | 7d |
| **Tag**         | OZONE, OM |
| **Description** | Maximum time a successful snapshot diff job and its report will be persisted. Uses millisecond by default when no time unit is specified. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.max.allowed.keys.changed.per.job` |
|:----------------|:----------------------------|
| **Value**       | 10000000 |
| **Tag**         | OZONE, OM |
| **Description** | Max numbers of keys changed allowed for a snapshot diff job. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.max.jobs.purge.per.task` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, OM |
| **Description** | Maximum number of snapshot diff jobs to be purged per snapDiff clean up run. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.max.page.size` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, OM |
| **Description** | Maximum number of entries to be returned in a single page of snap diff report. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.diff.thread.pool.size` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, OM |
| **Description** | Maximum numbers of concurrent snapshot diff jobs are allowed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.force.full.diff` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, OM |
| **Description** | Flag to always perform full snapshot diff (can be slow) without using the optimised compaction DAG. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.load.native.lib` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM |
| **Description** | Load native library for performing optimized snapshot diff. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.provider.connection.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5000s |
| **Tag**         | OZONE, OM, HA, MANAGEMENT |
| **Description** | Connection timeout for HTTP call made by OM Snapshot Provider to request OM snapshot from OM Leader. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.provider.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300000ms |
| **Tag**         | OZONE, OM, HA, MANAGEMENT |
| **Description** | Connection request timeout for HTTP call made by OM Snapshot Provider to request OM snapshot from OM Leader. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.snapshot.provider.socket.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5000s |
| **Tag**         | OZONE, OM, HA, MANAGEMENT |
| **Description** | Socket timeout for HTTP call made by OM Snapshot Provider to request OM snapshot from OM Leader. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.transport.class` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Property to determine the transport protocol for the client to Ozone Manager channel. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.unflushed.transaction.max.count` |
|:----------------|:----------------------------|
| **Value**       | 10000 |
| **Tag**         | OZONE, OM |
| **Description** | the unflushed transactions here are those requests that have been applied to OM state machine but not been flushed to OM rocksdb. when OM meets high concurrency-pressure and flushing is not fast enough, too many pending requests will be hold in memory and will lead to long GC of OM, which will slow down flushing further. there are some cases that flushing is slow, for example, 1 rocksdb is on a HDD, which has poor IO performance than SSD. 2 a big compaction is happening internally in rocksdb and write stall of rocksdb happens. 3 long GC, which may caused by other factors. the property is to limit the max count of unflushed transactions, so that the maximum memory occupied by unflushed transactions is limited. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.upgrade.quota.recalculate.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, OM |
| **Description** | quota recalculation trigger when upgrade to the layout version QUOTA. while upgrade, re-calculation of quota used will block write operation to existing buckets till this operation is completed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.user.max.volume` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | OM, MANAGEMENT |
| **Description** | The maximum number of volumes a user can have on a cluster.Increasing or decreasing this number has no real impact on ozone cluster. This is defined only for operational purposes. Only an administrator can create a volume, once a volume is created there are no restrictions on the number of buckets or keys inside each bucket a user can create. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.om.volume.listall.allowed` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OM, MANAGEMENT |
| **Description** | Allows everyone to list all volumes when set to true. Defaults to true. When set to false, non-admin users can only list the volumes they have access to. Admins can always list all volumes. Note that this config only applies to OzoneNativeAuthorizer. For other authorizers, admin needs to set policies accordingly to allow all volume listing e.g. for Ranger, a new policy with special volume "/" can be added to allow group public LIST access. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.path.deleting.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 6000 |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | A maximum number of paths(dirs/files) to be deleted by directory deleting service per time interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.readonly.administrators` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Ozone read only admin users delimited by the comma. If set, This is the list of users are allowed to read operations skip checkAccess. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.readonly.administrators.groups` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         |  |
| **Description** | Ozone read only admin groups delimited by the comma. If set, This is the list of groups are allowed to read operations skip checkAccess. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.administrators` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | RECON, SECURITY |
| **Description** | Recon administrator users delimited by a comma. This is the list of users who can access admin only information from recon. Users defined in ozone.administrators will always be able to access all recon information regardless of this setting. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.administrators.groups` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | RECON, SECURITY |
| **Description** | Recon administrator groups delimited by a comma. This is the list of groups who can access admin only information from recon. It is enough to either have the name defined in ozone.recon.administrators or be directly or indirectly in a group defined in this property. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.containerkey.flush.db.max.threshold` |
|:----------------|:----------------------------|
| **Value**       | 150000 |
| **Tag**         | OZONE, RECON, PERFORMANCE |
| **Description** | Maximum threshold number of entries to hold in memory for Container Key Mapper task in hashmap before flushing to recon rocks DB containerKeyTable |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.db.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, RECON, STORAGE, PERFORMANCE |
| **Description** | Directory where the Recon Server stores its metadata. This should be specified as a single directory. If the directory does not exist then the Recon will attempt to create it. If undefined, then the Recon will log a warning and fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.db.dirs.permissions` |
|:----------------|:----------------------------|
| **Value**       | 750 |
| **Tag**         |  |
| **Description** | Permissions for the metadata directories for Recon. The permissions can either be octal or symbolic. If the default permissions are not set then the default value of 750 will be used. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.heatmap.enable` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, RECON |
| **Description** | To enable/disable recon heatmap feature. Along with this config, user must also provide the implementation of "org.apache.hadoop.ozone.recon.heatmap.IHeatMapProvider" interface and configure in "ozone.recon.heatmap.provider" configuration. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.heatmap.provider` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, RECON |
| **Description** | Fully qualified heatmap provider implementation class name. If this value is not set, then HeatMap feature will be disabled and not exposed in Recon UI. Please refer Ozone doc for more details regarding the implementation of "org.apache.hadoop.ozone.recon.heatmap.IHeatMapProvider" interface. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9888 |
| **Tag**         | RECON, MANAGEMENT |
| **Description** | The address and the base port where the Recon web UI will listen on. If the port is 0, then the server will start on a free port. However, it is best to specify a well-known port, so it is easy to connect and see the Recon management UI. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | RECON, MANAGEMENT |
| **Description** | The actual address the Recon server will bind to. If this optional the address is set, it overrides only the hostname portion of ozone.recon.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | RECON, SECURITY, KERBEROS |
| **Description** | The keytab file for HTTP Kerberos authentication in Recon. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | RECON, SECURITY, KERBEROS |
| **Description** | The server principal used by Ozone Recon server. This is typically set to HTTP/_HOST@REALM.TLD The SPNEGO server principal begins with the prefix HTTP/ by convention. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | RECON, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | RECON, MANAGEMENT |
| **Description** | Property to enable or disable Recon web user interface. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.https-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9889 |
| **Tag**         | RECON, MANAGEMENT, SECURITY |
| **Description** | The address and the base port where the Recon web UI will listen on using HTTPS. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | RECON, MANAGEMENT, SECURITY |
| **Description** | The actual address the Recon web server will bind to using HTTPS. If this optional address is set, it overrides only the hostname portion of ozone.recon.https-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.nssummary.flush.db.max.threshold` |
|:----------------|:----------------------------|
| **Value**       | 150000 |
| **Tag**         | OZONE, RECON, PERFORMANCE |
| **Description** | Maximum threshold number of entries to hold in memory for NSSummary task in hashmap before flushing to recon rocks DB namespaceSummaryTable |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.connection.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5000 |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Connection request timeout in milliseconds for HTTP call made by Recon to request OM DB snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.connection.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Connection timeout for HTTP call in milliseconds made by Recon to request OM snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.db.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, RECON, STORAGE |
| **Description** | Directory where the Recon Server stores its OM snapshot DB. This should be specified as a single directory. If the directory does not exist then the Recon will attempt to create it. If undefined, then the Recon will log a warning and fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.snapshot.task.flush.param` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Request to flush the OM DB before taking checkpoint snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.snapshot.task.initial.delay` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Initial delay in MINUTES by Recon to request OM DB Snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.snapshot.task.interval.delay` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Interval in MINUTES by Recon to request OM DB Snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.om.socket.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, RECON, OM |
| **Description** | Socket timeout in milliseconds for HTTP call made by Recon to request OM snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.connection.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | Connection request timeout in milliseconds for HTTP call made by Recon to request SCM DB snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.connection.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | Connection timeout for HTTP call in milliseconds made by Recon to request SCM snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.container.threshold` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | Threshold value for the difference in number of containers in SCM and RECON. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.snapshot.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | If enabled, SCM DB Snapshot is taken by Recon. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.snapshot.task.initial.delay` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, MANAGEMENT, RECON |
| **Description** | Initial delay in MINUTES by Recon to request SCM DB Snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scm.snapshot.task.interval.delay` |
|:----------------|:----------------------------|
| **Value**       | 24h |
| **Tag**         | OZONE, MANAGEMENT, RECON |
| **Description** | Interval in MINUTES by Recon to request SCM DB Snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scmclient.failover.max.retry` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | Max retry count for SCM Client when failover happens. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scmclient.max.retry.timeout` |
|:----------------|:----------------------------|
| **Value**       | 6s |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | Max retry timeout for SCM Client when Recon connects to SCM. This config is used to dynamically compute the max retry count for SCM Client when failover happens. Check the SCMClientConfig class getRetryCount method. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.scmclient.rpc.timeout` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, RECON, SCM |
| **Description** | RpcClient timeout on waiting for the response from SCM when Recon connects to SCM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.recon.task.thread.count` |
|:----------------|:----------------------------|
| **Value**       | 1 |
| **Tag**         | OZONE, RECON |
| **Description** | The number of Recon Tasks that are waiting on updates from OM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.rest.client.http.connection.max` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, CLIENT |
| **Description** | This defines the overall connection limit for the connection pool used in RestClient. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.rest.client.http.connection.per-route.max` |
|:----------------|:----------------------------|
| **Value**       | 20 |
| **Tag**         | OZONE, CLIENT |
| **Description** | This defines the connection limit per one HTTP route/host. Total max connection is limited by ozone.rest.client.http.connection.max property. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3.administrators` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY |
| **Description** | S3 administrator users delimited by a comma. This is the list of users who can access admin only information from s3. If this property is empty then ozone.administrators will be able to access all s3 information regardless of this setting. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3.administrators.groups` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY |
| **Description** | S3 administrator groups delimited by a comma. This is the list of groups who can access admin only information from S3. It is enough to either have the name defined in ozone.s3.administrators or be directly or indirectly in a group defined in this property. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.client.buffer.size` |
|:----------------|:----------------------------|
| **Value**       | 4KB |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The size of the buffer which is for read block. (4KB by default). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.default.bucket.layout` |
|:----------------|:----------------------------|
| **Value**       | OBJECT_STORE |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The bucket layout that will be used when buckets are created through the S3 API. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.domain.name` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | List of Ozone S3Gateway domain names. If multiple domain names to be provided, they should be a "," separated. This parameter is only required when virtual host style pattern is followed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9878 |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The address and the base port where the Ozone S3Gateway Server will listen on. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The actual address the HTTP server will bind to. If this optional address is set, it overrides only the hostname portion of ozone.s3g.http-address. This is useful for making the Ozone S3Gateway HTTP server listen on all interfaces by setting it to 0.0.0.0. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http.auth.kerberos.keytab` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/HTTP.keytab |
| **Tag**         | OZONE, S3GATEWAY, SECURITY, KERBEROS |
| **Description** | The keytab file used by the S3Gateway server to login as its service principal. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http.auth.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | HTTP/_HOST@REALM |
| **Tag**         | OZONE, S3GATEWAY, SECURITY, KERBEROS |
| **Description** | The server principal used by Ozone S3Gateway server. This is typically set to HTTP/_HOST@REALM.TLD The SPNEGO server principal begins with the prefix HTTP/ by convention. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | simple |
| **Tag**         | S3GATEWAY, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, SPNEGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The boolean which enables the Ozone S3Gateway server . |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.https-address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | Ozone S3Gateway serverHTTPS server address and port . |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The actual address the HTTPS server will bind to. If this optional address is set, it overrides only the hostname portion of ozone.s3g.https-address. This is useful for making the Ozone S3Gateway HTTPS server listen on all interfaces by setting it to 0.0.0.0. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.kerberos.keytab.file` |
|:----------------|:----------------------------|
| **Value**       | /etc/security/keytabs/s3g.keytab |
| **Tag**         | OZONE, SECURITY, KERBEROS, S3GATEWAY |
| **Description** | The keytab file used by S3Gateway daemon to login as its service principal. The principal name is configured with ozone.s3g.kerberos.principal. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.kerberos.principal` |
|:----------------|:----------------------------|
| **Value**       | s3g/_HOST@REALM |
| **Tag**         | OZONE, SECURITY, KERBEROS, S3GATEWAY |
| **Description** | The S3Gateway service principal. Ex: s3g/_HOST@REALM.COM |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.list-keys.shallow.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | If this is true, there will be efficiency optimization effects when calling s3g list interface with delimiter '/' parameter, especially when there are a large number of keys. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.metrics.percentiles.intervals.seconds` |
|:----------------|:----------------------------|
| **Value**       | 60 |
| **Tag**         | S3GATEWAY, PERFORMANCE |
| **Description** | Specifies the interval in seconds for the rollover of MutableQuantiles metrics. Setting this interval equal to the metrics sampling time ensures more detailed metrics. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.secret.http.auth.type` |
|:----------------|:----------------------------|
| **Value**       | kerberos |
| **Tag**         | S3GATEWAY, SECURITY, KERBEROS |
| **Description** | simple or kerberos. If kerberos is set, Kerberos SPNEOGO will be used for http authentication. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.secret.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The boolean which enables the Ozone S3Gateway Secret endpoint. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.s3g.volume.name` |
|:----------------|:----------------------------|
| **Value**       | s3v |
| **Tag**         | OZONE, S3GATEWAY |
| **Description** | The volume name to access through the s3gateway. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.client.address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM |
| **Description** | The address of the Ozone SCM block client service. If not defined value of ozone.scm.client.address is used. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.client.bind.host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, SCM |
| **Description** | The hostname or IP address used by the SCM block client endpoint to bind. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.client.port` |
|:----------------|:----------------------------|
| **Value**       | 9863 |
| **Tag**         | OZONE, SCM |
| **Description** | The port number of the Ozone SCM block client service. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.deletion.max.retry` |
|:----------------|:----------------------------|
| **Value**       | 4096 |
| **Tag**         | OZONE, SCM |
| **Description** | SCM wraps up many blocks in a deletion transaction and sends that to data node for physical deletion periodically. This property determines how many times SCM is going to retry sending a deletion operation to the data node. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, MANAGEMENT, PERFORMANCE |
| **Description** | Used to set the number of RPC handlers when accessing blocks. The default value is 100. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.block.size` |
|:----------------|:----------------------------|
| **Value**       | 256MB |
| **Tag**         | OZONE, SCM |
| **Description** | The default size of a scm block. This is maps to the default Ozone block size. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ca.list.retry.interval` |
|:----------------|:----------------------------|
| **Value**       | 10s |
| **Tag**         | OZONE, SCM, OM, DATANODE |
| **Description** | SCM client wait duration between each retry to get Scm CA list. OM/Datanode obtain CA list during startup, and wait for the CA List size to be matched with SCM node count size plus 1. (Additional one certificate is root CA certificate). If the received CA list size is not matching with expected count, this is the duration used to wait before making next attempt to get CA list. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.chunk.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | OZONE, SCM, CONTAINER, PERFORMANCE |
| **Description** | The chunk size for reading/writing chunk operations in bytes. The chunk size defaults to 4MB. If the value configured is more than the maximum size (32MB), it will be reset to the maximum size (32MB). This maps to the network packet sizes and file write operations in the client to datanode protocol. When tuning this parameter, flow control window parameter should be tuned accordingly. Refer to hdds.ratis.raft.grpc.flow.control.window for more information. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, REQUIRED |
| **Description** | The address of the Ozone SCM client service. This is a required setting. It is a string in the host:port format. The port number is optional and defaults to 9860. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.address` |
|:----------------|:----------------------------|
| **Value**       | localhost |
| **Tag**         | MANAGEMENT |
| **Description** | Client address (To test string injection). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.bind.host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, SCM, MANAGEMENT |
| **Description** | The hostname or IP address used by the SCM client endpoint to bind. This setting is used by the SCM only and never used by clients. The setting can be useful in multi-homed setups to restrict the availability of the SCM client service to a specific interface. The default is appropriate for most clusters. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.bind.host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | MANAGEMENT |
| **Description** | Bind host(To test string injection). |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.compression.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | MANAGEMENT |
| **Description** | Compression enabled. (Just to test boolean flag) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.dynamic` |
|:----------------|:----------------------------|
| **Value**       | original |
| **Tag**         |  |
| **Description** | Test dynamic property |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.grandpa.dyna` |
|:----------------|:----------------------------|
| **Value**       | x |
| **Tag**         |  |
| **Description** | Test inherited dynamic property |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, MANAGEMENT, PERFORMANCE |
| **Description** | Used to set the number of RPC handlers used by Client to access SCM. The default value is 100. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.number` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | MANAGEMENT |
| **Description** | Example numeric configuration |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.port` |
|:----------------|:----------------------------|
| **Value**       | 9860 |
| **Tag**         | OZONE, SCM, MANAGEMENT |
| **Description** | The port number of the Ozone SCM client service. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.port` |
|:----------------|:----------------------------|
| **Value**       | 1234 |
| **Tag**         | MANAGEMENT |
| **Description** | Port number config (To test in injection) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.secure` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | MANAGEMENT |
| **Description** | Make everything secure. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.size.large` |
|:----------------|:----------------------------|
| **Value**       | 5GB |
| **Tag**         |  |
| **Description** | Testing SIZE with long field |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.size.small` |
|:----------------|:----------------------------|
| **Value**       | 42MB |
| **Tag**         |  |
| **Description** | Testing SIZE with int field |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.threshold` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | MANAGEMENT |
| **Description** | Threshold (To test DOUBLE config type) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.time.duration` |
|:----------------|:----------------------------|
| **Value**       | 1h |
| **Tag**         | MANAGEMENT |
| **Description** | N/A |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.client.wait` |
|:----------------|:----------------------------|
| **Value**       | 30m |
| **Tag**         | MANAGEMENT |
| **Description** | Wait time (To test TIME config type) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.close.container.wait.duration` |
|:----------------|:----------------------------|
| **Value**       | 150s |
| **Tag**         | SCM, OZONE, RECON |
| **Description** | Wait duration before which close container is send to DN. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.container.layout` |
|:----------------|:----------------------------|
| **Value**       | FILE_PER_BLOCK |
| **Tag**         | OZONE, SCM, CONTAINER, PERFORMANCE |
| **Description** | Container layout defines how chunks, blocks and containers are stored on disk. Each chunk is stored separately with FILE_PER_CHUNK. All chunks of a block are stored in the same file with FILE_PER_BLOCK. The default is FILE_PER_BLOCK. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.container.lock.stripes` |
|:----------------|:----------------------------|
| **Value**       | 512 |
| **Tag**         | OZONE, SCM, PERFORMANCE, MANAGEMENT |
| **Description** | The number of stripes created for the container state manager lock. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.container.placement.ec.impl` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The full name of class which implements org.apache.hadoop.hdds.scm.PlacementPolicy. The class decides which datanode will be used to host the container replica in EC mode. If not set, org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom will be used as default value. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.container.placement.impl` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The full name of class which implements org.apache.hadoop.hdds.scm.PlacementPolicy. The class decides which datanode will be used to host the container replica. If not set, org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom will be used as default value. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.container.size` |
|:----------------|:----------------------------|
| **Value**       | 5GB |
| **Tag**         | OZONE, PERFORMANCE, MANAGEMENT |
| **Description** | Default container size used by Ozone. There are two considerations while picking this number. The speed at which a container can be replicated, determined by the network speed and the metadata that each container generates. So selecting a large number creates less SCM metadata, but recovery time will be more. 5GB is a number that maps to quick replication times in gigabit networks, but still balances the amount of metadata. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The address of the Ozone SCM service used for internal communication between the DataNodes and the SCM. It is a string in the host:port format. The port number is optional and defaults to 9861. This setting is optional. If unspecified then the hostname portion is picked from the ozone.scm.client.address setting and the default service port of 9861 is chosen. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.admin.monitor.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | SCM |
| **Description** | This sets how frequently the datanode admin monitor runs to check for nodes added to the admin workflow or removed from it. The progress of decommissioning and entering maintenance nodes is also checked to see if they have completed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.admin.monitor.logging.limit` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | SCM |
| **Description** | When a node is checked for decommission or maintenance, this setting controls how many degraded containers are logged on each pass. The limit is applied separately for each type of container, ie under-replicated and unhealthy will each have their own limit. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.bind.host` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The hostname or IP address used by the SCM service endpoint to bind. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.disallow.same.peers` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Disallows same set of datanodes to participate in multiple pipelines when set to true. Default is set to false. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, MANAGEMENT, PERFORMANCE |
| **Description** | Used to set the number of RPC handlers used by DataNode to access SCM. The default value is 100. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.id.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The path that datanodes will use to store the datanode ID. If this value is not set, then datanode ID is created under the metadata directory. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.pipeline.limit` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Max number of pipelines per datanode can be engaged in. Setting the value to 0 means the pipeline limit per dn will be determined by the no of metadata volumes reported per dn. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.port` |
|:----------------|:----------------------------|
| **Value**       | 9861 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The port number of the Ozone SCM service. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.datanode.ratis.volume.free-space.min` |
|:----------------|:----------------------------|
| **Value**       | 1GB |
| **Tag**         | OZONE, DATANODE |
| **Description** | Minimum amount of storage space required for each ratis volume on a datanode to hold a new pipeline. Datanodes with all its ratis volumes with space under this value will not be allocated a pipeline or container replica. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.db.dirs` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, STORAGE, PERFORMANCE |
| **Description** | Directory where the StorageContainerManager stores its metadata. This should be specified as a single directory. If the directory does not exist then the SCM will attempt to create it. If undefined, then the SCM will log a warning and fallback to ozone.metadata.dirs. This fallback approach is not recommended for production environments. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.db.dirs.permissions` |
|:----------------|:----------------------------|
| **Value**       | 750 |
| **Tag**         |  |
| **Description** | Permissions for the metadata directories for Storage Container Manager. The permissions can either be octal or symbolic. If the default permissions are not set then the default value of 750 will be used. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.dead.node.interval` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The interval between heartbeats before a node is tagged as dead. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.default.service.id` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA |
| **Description** | Service ID of the SCM. If this is not set fall back to ozone.scm.service.ids to find the service ID it belongs to. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.event.ContainerReport.thread.pool.size` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, SCM |
| **Description** | Thread pool size configured to process container reports. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.expired.container.replica.op.scrub.interval` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, SCM, CONTAINER |
| **Description** | SCM schedules a fixed interval job using the configured interval to scrub expired container replica operation. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.grpc.port` |
|:----------------|:----------------------------|
| **Value**       | 9895 |
| **Tag**         | OZONE, SCM, HA, RATIS |
| **Description** | The port number of the SCM's grpc server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.dbtransactionbuffer.flush.interval` |
|:----------------|:----------------------------|
| **Value**       | 600s |
| **Tag**         | SCM, OZONE |
| **Description** | Wait duration for flush of buffered transaction. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.grpc.deadline.interval` |
|:----------------|:----------------------------|
| **Value**       | 30m |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Deadline for SCM DB checkpoint interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.leader.election.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The minimum timeout duration for SCM ratis leader election. Default is 1s. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.leader.ready.check.interval` |
|:----------------|:----------------------------|
| **Value**       | 2s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The interval between ratis server performing a leader readiness check. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.leader.ready.wait.timeout` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The minimum timeout duration for waiting for leader readiness. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.log.appender.queue.byte-limit` |
|:----------------|:----------------------------|
| **Value**       | 32MB |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Byte limit for Raft's Log Worker queue. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.log.appender.queue.num-elements` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Number of operation pending with Raft's Log Worker. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.log.purge.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | whether enable raft log purge. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.log.purge.gap` |
|:----------------|:----------------------------|
| **Value**       | 1000000 |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The minimum gap between log indices for Raft server to purge its log segments after taking snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.request.timeout` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The timeout duration for SCM's Ratis server RPC. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.rpc.type` |
|:----------------|:----------------------------|
| **Value**       | GRPC |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Ratis supports different kinds of transports like netty, GRPC， Hadoop RPC etc. This picks one of those for this cluster. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.segment.preallocated.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The size of the buffer which is preallocated for raft segment used by Apache Ratis on SCM.(4 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.segment.size` |
|:----------------|:----------------------------|
| **Value**       | 4MB |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The size of the raft segment used by Apache Ratis on SCM. (4 MB by default) |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.server.failure.timeout.duration` |
|:----------------|:----------------------------|
| **Value**       | 120s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The timeout duration for ratis server failure detection, once the threshold has reached, the ratis state machine will be informed about the failure in the ratis ring. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.server.leaderelection.pre-vote` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Enable/disable SCM HA leader election pre-vote phase. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.server.retry.cache.timeout` |
|:----------------|:----------------------------|
| **Value**       | 60s |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | Retry Cache entry timeout for SCM's Ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.server.snapshot.creation.gap` |
|:----------------|:----------------------------|
| **Value**       | 1024 |
| **Tag**         | SCM, OZONE |
| **Description** | Raft snapshot gap index after which snapshot can be taken. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.snapshot.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The ratis snapshot dir location. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.snapshot.threshold` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | SCM, OZONE, HA, RATIS |
| **Description** | The threshold to trigger a Ratis taking snapshot operation for SCM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ha.ratis.storage.dir` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA, RATIS |
| **Description** | Storage directory used by SCM to write Ratis logs. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 100 |
| **Tag**         | OZONE, MANAGEMENT, PERFORMANCE |
| **Description** | The number of RPC handler threads for each SCM service endpoint. The default is appropriate for small clusters (tens of nodes). Set a value that is appropriate for the cluster size. Generally, HDFS recommends RPC handler count is set to 20 * log2(Cluster Size) with an upper limit of 200. However, Ozone SCM will not have the same amount of traffic as HDFS Namenode, so a value much smaller than that will work well too. To specify handlers for individual RPC servers, set the following configuration properties instead: ---- RPC type ---- : ---- Configuration properties ---- SCMClientProtocolServer : 'ozone.scm.client.handler.count.key' SCMBlockProtocolServer : 'ozone.scm.block.handler.count.key' SCMDatanodeProtocolServer: 'ozone.scm.datanode.handler.count.key' |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.heartbeat.log.warn.interval.count` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Defines how frequently we will log the missing of a heartbeat to SCM. For example in the default case, we will write a warning message for each ten consecutive heartbeats that we miss to SCM. This helps in reducing clutter in a data node log, but trade off is that logs will have less of this statement. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.heartbeat.rpc-retry-count` |
|:----------------|:----------------------------|
| **Value**       | 15 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Retry count for the RPC from Datanode to SCM. The rpc-retry-interval is 1s by default. Make sure rpc-retry-count * (rpc-timeout + rpc-retry-interval) is less than hdds.heartbeat.interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.heartbeat.rpc-retry-interval` |
|:----------------|:----------------------------|
| **Value**       | 1s |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Retry interval for the RPC from Datanode to SCM. Make sure rpc-retry-count * (rpc-timeout + rpc-retry-interval) is less than hdds.heartbeat.interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.heartbeat.rpc-timeout` |
|:----------------|:----------------------------|
| **Value**       | 5s |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Timeout value for the RPC from Datanode to SCM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.heartbeat.thread.interval` |
|:----------------|:----------------------------|
| **Value**       | 3s |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | When a heartbeat from the data node arrives on SCM, It is queued for processing with the time stamp of when the heartbeat arrived. There is a heartbeat processing thread inside SCM that runs at a specified interval. This value controls how frequently this thread is run. There are some assumptions build into SCM such as this value should allow the heartbeat processing thread to run at least three times more frequently than heartbeats and at least five times more than stale node detection time. If you specify a wrong value, SCM will gracefully refuse to run. For more info look at the node manager tests in SCM. In short, you don't need to change this. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.http-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9876 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The address and the base port where the SCM web ui will listen on. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.http-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The actual address the SCM web server will bind to. If this optional address is set, it overrides only the hostname portion of ozone.scm.http-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.http.enabled` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | Property to enable or disable SCM web ui. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.https-address` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0:9877 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The address and the base port where the SCM web UI will listen on using HTTPS. If the port is 0 then the server will start on a free port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.https-bind-host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The actual address the SCM web server will bind to using HTTPS. If this optional address is set, it overrides only the hostname portion of ozone.scm.https-address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.info.wait.duration` |
|:----------------|:----------------------------|
| **Value**       | 10m |
| **Tag**         | OZONE, SCM, OM |
| **Description** | Maximum amount of duration OM/SCM waits to get Scm Info/Scm signed cert during OzoneManager init/SCM bootstrap. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.keyvalue.container.deletion-choosing.policy` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The policy used for choosing desired keyvalue containers for block deletion. Datanode selects some containers to process block deletion in a certain interval defined by ozone.block.deleting.service.interval. The number of containers to process in each interval is defined by ozone.block.deleting.container.limit.per.interval. This property is used to configure the policy applied while selecting containers. There are two policies supporting now: RandomContainerDeletionChoosingPolicy and TopNOrderedContainerDeletionChoosingPolicy. org.apache.hadoop.ozone.container.common.impl.RandomContainerDeletionChoosingPolicy implements a simply random policy that to return a random list of containers. org.apache.hadoop.ozone.container.common.impl.TopNOrderedContainerDeletionChoosingPolicy implements a policy that choosing top count number of containers in a pending-deletion-blocks's num based descending order. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.names` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, REQUIRED |
| **Description** | The value of this property is a set of DNS | DNS:PORT | IP Address | IP:PORT. Written as a comma separated string. e.g. scm1, scm2:8020, 7.7.7.7:7777. This property allows datanodes to discover where SCM is, so that datanodes can send heartbeat to SCM. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.network.topology.schema.file` |
|:----------------|:----------------------------|
| **Value**       | network-topology-default.xml |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The schema file defines the ozone network topology. We currently support xml(default) and yaml format. Refer to the samples in the topology awareness document for xml and yaml topology definition samples. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.node.id` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA |
| **Description** | The ID of this SCM node. If the SCM node ID is not configured it is determined automatically by matching the local node's address with the configured address. If node ID is not deterministic from the configuration, then it is set to the scmId from the SCM version file. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.nodes.EXAMPLESCMSERVICEID` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA |
| **Description** | Comma-separated list of SCM node Ids for a given SCM service ID (eg. EXAMPLESCMSERVICEID). The SCM service ID should be the value (one of the values if there are multiple) set for the parameter ozone.scm.service.ids. Unique identifiers for each SCM Node, delimited by commas. This will be used by SCMs in HA setup to determine all the SCMs belonging to the same SCM in the cluster. For example, if you used “scmService1” as the SCM service ID previously, and you wanted to use “scm1”, “scm2” and "scm3" as the individual IDs of the SCMs, you would configure a property ozone.scm.nodes.scmService1, and its value "scm1,scm2,scm3". |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.allocated.timeout` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Timeout for every pipeline to stay in ALLOCATED stage. When pipeline is created, it should be at OPEN stage once pipeline report is successfully received by SCM. If a pipeline stays at ALLOCATED longer than the specified period of time, it should be scrubbed so that new pipeline can be created. This timeout is for how long pipeline can stay at ALLOCATED stage until it gets scrubbed. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.creation.auto.factor.one` |
|:----------------|:----------------------------|
| **Value**       | true |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | If enabled, SCM will auto create RATIS factor ONE pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.creation.interval` |
|:----------------|:----------------------------|
| **Value**       | 120s |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | SCM schedules a fixed interval job using the configured interval to create pipelines. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.destroy.timeout` |
|:----------------|:----------------------------|
| **Value**       | 66s |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Once a pipeline is closed, SCM should wait for the above configured time before destroying a pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.leader-choose.policy` |
|:----------------|:----------------------------|
| **Value**       | org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.MinLeaderCountChoosePolicy |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | The policy used for choosing desired leader for pipeline creation. There are two policies supporting now: DefaultLeaderChoosePolicy, MinLeaderCountChoosePolicy. org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.DefaultLeaderChoosePolicy implements a policy that choose leader without depending on priority. org.apache.hadoop.hdds.scm.pipeline.leader.choose.algorithms.MinLeaderCountChoosePolicy implements a policy that choose leader which has the minimum exist leader count. In the future, we need to add policies which consider: 1. resource, the datanode with the most abundant cpu and memory can be made the leader 2. topology, the datanode nearest to the client can be made the leader |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.owner.container.count` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Number of containers per owner per disk in a pipeline. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.per.metadata.disk` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Number of pipelines to be created per raft log disk. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.pipeline.scrub.interval` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | SCM schedules a fixed interval job using the configured interval to scrub pipelines. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.primordial.node.id` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA |
| **Description** | optional config, if being set will cause scm --init to only take effect on the specific node and ignore scm --bootstrap cmd. Similarly, scm --init will be ignored on the non-primordial scm nodes. The config can either be set equal to the hostname or the node id of any of the scm nodes. With the config set, applications/admins can safely execute init and bootstrap commands safely on all scm instances. If a cluster is upgraded from non-ratis to ratis based SCM, scm --init needs to re-run for switching from non-ratis based SCM to ratis-based SCM on the primary node. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ratis.pipeline.limit` |
|:----------------|:----------------------------|
| **Value**       | 0 |
| **Tag**         | OZONE, SCM, PIPELINE |
| **Description** | Upper limit for how many pipelines can be OPEN in SCM. 0 as default means there is no limit. Otherwise, the number is the limit of max amount of pipelines which are OPEN. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.ratis.port` |
|:----------------|:----------------------------|
| **Value**       | 9894 |
| **Tag**         | OZONE, SCM, HA, RATIS |
| **Description** | The port number of the SCM's Ratis server. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.security.handler.count.key` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Threads configured for SCMSecurityProtocolServer. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.security.service.address` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | Address of SCMSecurityProtocolServer. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.security.service.bind.host` |
|:----------------|:----------------------------|
| **Value**       | 0.0.0.0 |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | SCM security server host. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.security.service.port` |
|:----------------|:----------------------------|
| **Value**       | 9961 |
| **Tag**         | OZONE, HDDS, SECURITY |
| **Description** | SCM security server port. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.sequence.id.batch.size` |
|:----------------|:----------------------------|
| **Value**       | 1000 |
| **Tag**         | OZONE, SCM |
| **Description** | SCM allocates sequence id in a batch way. This property determines how many ids will be allocated in a single batch. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.service.ids` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SCM, HA |
| **Description** | Comma-separated list of SCM service Ids. This property allows the client to figure out quorum of OzoneManager address. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.skip.bootstrap.validation` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SCM, HA |
| **Description** | optional config, the config when set to true skips the clusterId validation from leader scm during bootstrap |
--------------------------------------------------------------------------------
| **Name**        | `ozone.scm.stale.node.interval` |
|:----------------|:----------------------------|
| **Value**       | 5m |
| **Tag**         | OZONE, MANAGEMENT |
| **Description** | The interval for stale node flagging. Please see ozone.scm.heartbeat.thread.interval before changing this value. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.security.crypto.compliance.mode` |
|:----------------|:----------------------------|
| **Value**       | unrestricted |
| **Tag**         | OZONE, SECURITY, HDDS, CRYPTO_COMPLIANCE |
| **Description** | Based on this property the security compliance mode is loaded and enables filtering cryptographic configuration options according to the specified compliance mode. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.security.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | True if security is enabled for ozone. When this property is true, hadoop.security.authentication should be Kerberos. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.security.http.kerberos.enabled` |
|:----------------|:----------------------------|
| **Value**       | false |
| **Tag**         | OZONE, SECURITY, KERBEROS |
| **Description** | True if Kerberos authentication for Ozone HTTP web consoles is enabled using the SPNEGO protocol. When this property is true, hadoop.security.authentication should be Kerberos and ozone.security.enabled should be set to true. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.security.reconfigure.protocol.acl` |
|:----------------|:----------------------------|
| **Value**       | * |
| **Tag**         | SECURITY |
| **Description** | Comma separated list of users and groups allowed to access reconfigure protocol. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.server.default.replication` |
|:----------------|:----------------------------|
| **Value**       | 3 |
| **Tag**         | OZONE |
| **Description** | Default replication value. The actual number of replications can be specified when writing the key. The default is used if replication is not specified when creating key or no default replication set at bucket. Supported values: For Standalone: 1 For Ratis: 3 For Erasure Coding(EC) supported format: {ECCodec}-{DataBlocks}-{ParityBlocks}-{ChunkSize} ECCodec: Codec for encoding stripe. Supported values : XOR, RS (Reed Solomon) DataBlocks: Number of data blocks in a stripe. ParityBlocks: Number of parity blocks in a stripe. ChunkSize: Chunk size in bytes. E.g. 1024k, 2048k etc. Supported combinations of {DataBlocks}-{ParityBlocks} : 3-2, 6-3, 10-4 |
--------------------------------------------------------------------------------
| **Name**        | `ozone.server.default.replication.type` |
|:----------------|:----------------------------|
| **Value**       | RATIS |
| **Tag**         | OZONE |
| **Description** | Default replication type to be used while writing key into ozone. The value can be specified when writing the key, default is used when nothing is specified when creating key or no default value set at bucket. Supported values: RATIS, STAND_ALONE, CHAINED and EC. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.deleting.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | The maximum number of snapshots that would be reclaimed by Snapshot Deleting Service per run. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.deleting.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 30s |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | The time interval between successive SnapshotDeletingService thread run. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.deleting.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | Timeout value for SnapshotDeletingService. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.directory.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 24h |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | The time interval between successive SnapshotDirectoryCleaningService thread run. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.directory.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300s |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | Timeout value for SnapshotDirectoryCleaningService. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.filtering.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 2 |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | A maximum number of snapshots to be filtered by sst filtering service per time interval. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.filtering.service.interval` |
|:----------------|:----------------------------|
| **Value**       | 1m |
| **Tag**         | OZONE, PERFORMANCE, OM |
| **Description** | Time interval of the SST File filtering service from Snapshot. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.snapshot.key.deleting.limit.per.task` |
|:----------------|:----------------------------|
| **Value**       | 20000 |
| **Tag**         | OM, PERFORMANCE |
| **Description** | The maximum number of deleted keys to be scanned by Snapshot Deleting Service per snapshot run. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.sst.filtering.service.timeout` |
|:----------------|:----------------------------|
| **Value**       | 300000ms |
| **Tag**         | OZONE, PERFORMANCE,OM |
| **Description** | A timeout value of sst filtering service. |
--------------------------------------------------------------------------------
| **Name**        | `ozone.xceiver.client.metrics.percentiles.intervals.seconds` |
|:----------------|:----------------------------|
| **Value**       | 60 |
| **Tag**         | XCEIVER, PERFORMANCE |
| **Description** | Specifies the interval in seconds for the rollover of XceiverClient MutableQuantiles metrics. Setting this interval equal to the metrics sampling time ensures more detailed metrics. |
--------------------------------------------------------------------------------
| **Name**        | `recon.om.delta.update.limit` |
|:----------------|:----------------------------|
| **Value**       | 2000 |
| **Tag**         | OZONE, RECON |
| **Description** | Recon each time get a limited delta updates from OM. The actual fetched data might be larger than this limit. |
--------------------------------------------------------------------------------
| **Name**        | `recon.om.delta.update.loop.limit` |
|:----------------|:----------------------------|
| **Value**       | 10 |
| **Tag**         | OZONE, RECON |
| **Description** | The sync between Recon and OM consists of several small fetch loops. |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.keystore.keypassword` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Keystore key password for HTTPS SSL configuration |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.keystore.location` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Keystore location for HTTPS SSL configuration |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.keystore.password` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Keystore password for HTTPS SSL configuration |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.keystore.type` |
|:----------------|:----------------------------|
| **Value**       | jks |
| **Tag**         | OZONE, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | The keystore type for HTTP Servers used in ozone. |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.truststore.location` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Truststore location for HTTPS SSL configuration |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.truststore.password` |
|:----------------|:----------------------------|
| **Value**       |  |
| **Tag**         | OZONE, SECURITY, MANAGEMENT |
| **Description** | Truststore password for HTTPS SSL configuration |
--------------------------------------------------------------------------------
| **Name**        | `ssl.server.truststore.type` |
|:----------------|:----------------------------|
| **Value**       | jks |
| **Tag**         | OZONE, SECURITY, CRYPTO_COMPLIANCE |
| **Description** | The truststore type for HTTP Servers used in ozone. |
--------------------------------------------------------------------------------
