/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.ratis.util.TimeDuration;

/**
 * This class contains constants for configuration keys used in SCM.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ScmConfigKeys {
  public static final String OZONE_SCM_HA_PREFIX = "ozone.scm.ha";

  // Location of SCM DB files. For now we just support a single
  // metadata dir but in future we may support multiple for redundancy or
  // performance.
  public static final String OZONE_SCM_DB_DIRS = "ozone.scm.db.dirs";

  // SCM DB directory permission
  public static final String OZONE_SCM_DB_DIRS_PERMISSIONS =
      "ozone.scm.db.dirs.permissions";

  public static final String HDDS_CONTAINER_RATIS_ENABLED_KEY
      = "hdds.container.ratis.enabled";
  public static final boolean HDDS_CONTAINER_RATIS_ENABLED_DEFAULT
      = false;
  public static final String HDDS_CONTAINER_RATIS_RPC_TYPE_KEY
      = "hdds.container.ratis.rpc.type";
  public static final String HDDS_CONTAINER_RATIS_RPC_TYPE_DEFAULT
      = "GRPC";
  public static final String
      HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME
      = "hdds.container.ratis.num.write.chunk.threads.per.volume";
  public static final int
      HDDS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_DEFAULT
      = 10;
  public static final String HDDS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY
      = "hdds.container.ratis.num.container.op.executors";
  public static final int HDDS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_DEFAULT
      = 10;
  public static final String HDDS_CONTAINER_RATIS_SEGMENT_SIZE_KEY =
      "hdds.container.ratis.segment.size";
  public static final String HDDS_CONTAINER_RATIS_SEGMENT_SIZE_DEFAULT =
      "64MB";
  public static final String HDDS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY =
      "hdds.container.ratis.segment.preallocated.size";
  public static final String
      HDDS_CONTAINER_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT = "4MB";
  public static final String
      HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT =
      "hdds.container.ratis.statemachinedata.sync.timeout";
  public static final TimeDuration
      HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_TIMEOUT_DEFAULT =
      TimeDuration.valueOf(10, TimeUnit.SECONDS);
  public static final String
      HDDS_CONTAINER_RATIS_STATEMACHINEDATA_SYNC_RETRIES =
      "hdds.container.ratis.statemachinedata.sync.retries";
  public static final String
      HDDS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS =
      "hdds.container.ratis.statemachine.max.pending.apply-transactions";
  // The default value of maximum number of pending state machine apply
  // transactions is kept same as default snapshot threshold.
  public static final int
      HDDS_CONTAINER_RATIS_STATEMACHINE_MAX_PENDING_APPLY_TXNS_DEFAULT =
      100000;
  public static final String HDDS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS =
      "hdds.container.ratis.log.queue.num-elements";
  public static final int HDDS_CONTAINER_RATIS_LOG_QUEUE_NUM_ELEMENTS_DEFAULT =
      1024;
  public static final String HDDS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT =
      "hdds.container.ratis.log.queue.byte-limit";
  public static final String HDDS_CONTAINER_RATIS_LOG_QUEUE_BYTE_LIMIT_DEFAULT =
      "4GB";
  public static final String
      HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS =
      "hdds.container.ratis.log.appender.queue.num-elements";
  public static final int
      HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT = 1024;
  public static final String HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT =
      "hdds.container.ratis.log.appender.queue.byte-limit";
  public static final String
      HDDS_CONTAINER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB";
  public static final String HDDS_CONTAINER_RATIS_LOG_PURGE_GAP =
      "hdds.container.ratis.log.purge.gap";
  // TODO: Set to 1024 once RATIS issue around purge is fixed.
  public static final int HDDS_CONTAINER_RATIS_LOG_PURGE_GAP_DEFAULT =
      1000000;
  public static final String HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT =
      "hdds.container.ratis.leader.pending.bytes.limit";
  public static final String
      HDDS_CONTAINER_RATIS_LEADER_PENDING_BYTES_LIMIT_DEFAULT = "1GB";

  public static final String HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_KEY =
      "hdds.ratis.server.retry-cache.timeout.duration";
  public static final TimeDuration
      HDDS_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
  public static final String
      HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_KEY =
      "hdds.ratis.leader.election.minimum.timeout.duration";
  public static final TimeDuration
      HDDS_RATIS_LEADER_ELECTION_MINIMUM_TIMEOUT_DURATION_DEFAULT =
      TimeDuration.valueOf(5, TimeUnit.SECONDS);

  public static final String HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY =
      "hdds.ratis.snapshot.threshold";
  public static final long HDDS_RATIS_SNAPSHOT_THRESHOLD_DEFAULT = 100000;

  public static final String OZONE_SCM_CONTAINER_LIST_MAX_COUNT =
      "ozone.scm.container.list.max.count";

  public static final int OZONE_SCM_CONTAINER_LIST_MAX_COUNT_DEFAULT = 4096;

  // TODO : this is copied from OzoneConsts, may need to move to a better place
  public static final String OZONE_SCM_CHUNK_SIZE_KEY = "ozone.scm.chunk.size";
  // 4 MB by default
  public static final String OZONE_SCM_CHUNK_SIZE_DEFAULT = "4MB";

  public static final String OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_KEY =
      "ozone.chunk.read.buffer.default.size";
  public static final String OZONE_CHUNK_READ_BUFFER_DEFAULT_SIZE_DEFAULT =
      "1MB";
  public static final String OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_KEY =
      "ozone.chunk.read.mapped.buffer.threshold";
  public static final String OZONE_CHUNK_READ_MAPPED_BUFFER_THRESHOLD_DEFAULT =
      "32KB";
  public static final String OZONE_CHUNK_READ_MAPPED_BUFFER_MAX_COUNT_KEY =
      "ozone.chunk.read.mapped.buffer.max.count";
  // this max_count could not be greater than Linux platform max_map_count which by default is 65530.
  public static final int OZONE_CHUNK_READ_MAPPED_BUFFER_MAX_COUNT_DEFAULT = 0;
  public static final String OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_KEY =
      "ozone.chunk.read.netty.ChunkedNioFile";
  public static final boolean OZONE_CHUNK_READ_NETTY_CHUNKED_NIO_FILE_DEFAULT = false;

  public static final String OZONE_SCM_CONTAINER_LAYOUT_KEY =
      "ozone.scm.container.layout";

  public static final String OZONE_SCM_CLIENT_PORT_KEY =
      "ozone.scm.client.port";
  public static final int OZONE_SCM_CLIENT_PORT_DEFAULT = 9860;

  public static final String OZONE_SCM_DATANODE_PORT_KEY =
      "ozone.scm.datanode.port";
  public static final int OZONE_SCM_DATANODE_PORT_DEFAULT = 9861;

  // OZONE_OM_PORT_DEFAULT = 9862
  public static final String OZONE_SCM_BLOCK_CLIENT_PORT_KEY =
      "ozone.scm.block.client.port";
  public static final int OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT = 9863;

  public static final String OZONE_SCM_SECURITY_SERVICE_PORT_KEY =
      "ozone.scm.security.service.port";
  public static final int OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT = 9961;

  // Container service client
  public static final String OZONE_SCM_CLIENT_ADDRESS_KEY =
      "ozone.scm.client.address";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_KEY =
      "ozone.scm.client.bind.host";
  public static final String OZONE_SCM_CLIENT_BIND_HOST_DEFAULT =
      "0.0.0.0";

  // Block service client
  public static final String OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY =
      "ozone.scm.block.client.address";
  public static final String OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY =
      "ozone.scm.block.client.bind.host";
  public static final String OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT =
      "0.0.0.0";

  // SCM Security service address.
  public static final String OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY =
      "ozone.scm.security.service.address";
  public static final String OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY =
      "ozone.scm.security.service.bind.host";
  public static final String OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_DATANODE_ADDRESS_KEY =
      "ozone.scm.datanode.address";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_KEY =
      "ozone.scm.datanode.bind.host";
  public static final String OZONE_SCM_DATANODE_BIND_HOST_DEFAULT =
      "0.0.0.0";

  public static final String OZONE_SCM_HTTP_ENABLED_KEY =
      "ozone.scm.http.enabled";
  public static final String OZONE_SCM_HTTP_BIND_HOST_KEY =
      "ozone.scm.http-bind-host";
  public static final String OZONE_SCM_HTTPS_BIND_HOST_KEY =
      "ozone.scm.https-bind-host";
  public static final String OZONE_SCM_HTTP_ADDRESS_KEY =
      "ozone.scm.http-address";
  public static final String OZONE_SCM_HTTPS_ADDRESS_KEY =
      "ozone.scm.https-address";

  public static final String OZONE_SCM_ADDRESS_KEY =
      "ozone.scm.address";
  public static final String OZONE_SCM_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final String OZONE_SCM_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_SCM_HTTP_BIND_PORT_DEFAULT = 9876;
  public static final int OZONE_SCM_HTTPS_BIND_PORT_DEFAULT = 9877;
  public static final String HDDS_DATANODE_DIR_KEY = "hdds.datanode.dir";
  public static final String HDDS_DATANODE_DIR_DU_RESERVED =
      "hdds.datanode.dir.du.reserved";
  public static final String HDDS_DATANODE_DIR_DU_RESERVED_PERCENT =
      "hdds.datanode.dir.du.reserved.percent";
  public static final float HDDS_DATANODE_DIR_DU_RESERVED_PERCENT_DEFAULT = 0.0001f;
  public static final String OZONE_SCM_HANDLER_COUNT_KEY =
      "ozone.scm.handler.count.key";
  public static final String OZONE_SCM_CLIENT_HANDLER_COUNT_KEY =
      "ozone.scm.client.handler.count.key";
  public static final String OZONE_SCM_CLIENT_READ_THREADPOOL_KEY =
      "ozone.scm.client.read.threadpool";
  public static final int OZONE_SCM_CLIENT_READ_THREADPOOL_DEFAULT = 10;
  public static final String OZONE_SCM_BLOCK_HANDLER_COUNT_KEY =
      "ozone.scm.block.handler.count.key";
  public static final String OZONE_SCM_BLOCK_READ_THREADPOOL_KEY =
      "ozone.scm.block.read.threadpool";
  public static final int OZONE_SCM_BLOCK_READ_THREADPOOL_DEFAULT = 10;
  public static final String OZONE_SCM_DATANODE_HANDLER_COUNT_KEY =
      "ozone.scm.datanode.handler.count.key";
  public static final String OZONE_SCM_DATANODE_READ_THREADPOOL_KEY =
      "ozone.scm.datanode.read.threadpool";
  public static final int OZONE_SCM_DATANODE_READ_THREADPOOL_DEFAULT = 10;
  public static final int OZONE_SCM_HANDLER_COUNT_DEFAULT = 100;

  public static final String OZONE_SCM_SECURITY_HANDLER_COUNT_KEY =
      "ozone.scm.security.handler.count.key";
  public static final int OZONE_SCM_SECURITY_HANDLER_COUNT_DEFAULT = 2;
  public static final String OZONE_SCM_SECURITY_READ_THREADPOOL_KEY =
      "ozone.scm.security.read.threadpool";
  public static final int OZONE_SCM_SECURITY_READ_THREADPOOL_DEFAULT = 1;

  public static final String OZONE_SCM_DEADNODE_INTERVAL =
      "ozone.scm.dead.node.interval";
  public static final String OZONE_SCM_DEADNODE_INTERVAL_DEFAULT =
      "10m";

  public static final String OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL =
      "ozone.scm.heartbeat.thread.interval";
  public static final String OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_DEFAULT =
      "3s";

  public static final String OZONE_SCM_STALENODE_INTERVAL =
      "ozone.scm.stale.node.interval";
  public static final String OZONE_SCM_STALENODE_INTERVAL_DEFAULT =
      "5m";

  public static final String OZONE_SCM_HEARTBEAT_RPC_TIMEOUT =
      "ozone.scm.heartbeat.rpc-timeout";
  public static final String OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT =
      "5s";

  public static final String OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT =
      "ozone.scm.heartbeat.rpc-retry-count";
  public static final int OZONE_SCM_HEARTBEAT_RPC_RETRY_COUNT_DEFAULT =
      15;

  public static final String OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL =
      "ozone.scm.heartbeat.rpc-retry-interval";
  public static final String OZONE_SCM_HEARTBEAT_RPC_RETRY_INTERVAL_DEFAULT =
      "1s";

  /**
   * Defines how frequently we will log the missing of heartbeat to a specific
   * SCM. In the default case we will write a warning message for each 10
   * sequential heart beats that we miss to a specific SCM. This is to avoid
   * overrunning the log with lots of HB missed Log statements.
   */
  public static final String OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT =
      "ozone.scm.heartbeat.log.warn.interval.count";
  public static final int OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT =
      10;

  // ozone.scm.names key is a set of DNS | DNS:PORT | IP Address | IP:PORT.
  // Written as a comma separated string. e.g. scm1, scm2:8020, 7.7.7.7:7777
  //
  // If this key is not specified datanodes will not be able to find
  // SCM. The SCM membership can be dynamic, so this key should contain
  // all possible SCM names. Once the SCM leader is discovered datanodes will
  // get the right list of SCMs to heartbeat to from the leader.
  // While it is good for the datanodes to know the names of all SCM nodes,
  // it is sufficient to actually know the name of on working SCM. That SCM
  // will be able to return the information about other SCMs that are part of
  // the SCM replicated Log.
  //
  //In case of a membership change, any one of the SCM machines will be
  // able to send back a new list to the datanodes.
  public static final String OZONE_SCM_NAMES = "ozone.scm.names";

  public static final String OZONE_SCM_DEFAULT_SERVICE_ID =
      "ozone.scm.default.service.id";

  public static final String OZONE_SCM_SERVICE_IDS_KEY =
      "ozone.scm.service.ids";
  public static final String OZONE_SCM_NODES_KEY =
      "ozone.scm.nodes";
  public static final String OZONE_SCM_NODE_ID_KEY =
      "ozone.scm.node.id";

  /**
   * Optional config, if being set will cause scm --init to only take effect on
   * the specific node and ignore scm --bootstrap cmd.
   * Similarly, scm --init will be ignored on the non-primordial scm nodes.
   * With the config set, applications/admins can safely execute init and
   * bootstrap commands safely on all scm instances, for example kubernetes
   * deployments.
   *
   * If a cluster is upgraded from non-ratis to ratis based SCM, scm --init
   * needs to re-run for switching from
   * non-ratis based SCM to ratis-based SCM on the primary node.
   */
  public static final String OZONE_SCM_PRIMORDIAL_NODE_ID_KEY =
      "ozone.scm.primordial.node.id";

  /**
   * The config when set to true skips the clusterId validation from leader
   * scm during bootstrap. In SCM HA, the primary node starts up the ratis
   * server while other bootstrapping nodes will get added to the ratis group.
   * Now, if all the bootstrapping SCM get stopped post the group formation,
   * the primary node will now step down from leadership as it will loose
   * majority. If the bootstrapping nodes are now bootstrapped again,
   * the bootstrapping node will try to first validate the cluster id from the
   * leader SCM with the persisted cluster id , but as there is no leader
   * existing, bootstrapping will keep on failing and retrying until
   * it shuts down.
   */
  public static final String OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY =
      "ozone.scm.skip.bootstrap.validation";
  public static final boolean OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_DEFAULT =
      false;
  // The path where datanode ID is to be written to.
  // if this value is not set then container startup will fail.
  public static final String OZONE_SCM_DATANODE_ID_DIR =
      "ozone.scm.datanode.id.dir";

  public static final String OZONE_SCM_CONTAINER_SIZE =
      "ozone.scm.container.size";
  public static final String OZONE_SCM_CONTAINER_SIZE_DEFAULT = "5GB";

  public static final String OZONE_SCM_CONTAINER_LOCK_STRIPE_SIZE =
      "ozone.scm.container.lock.stripes";
  public static final int OZONE_SCM_CONTAINER_LOCK_STRIPE_SIZE_DEFAULT = 512;

  public static final String OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY =
      "ozone.scm.container.placement.impl";
  public static final String OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY =
      "ozone.scm.pipeline.placement.impl";
  public static final String OZONE_SCM_CONTAINER_PLACEMENT_EC_IMPL_KEY =
      "ozone.scm.container.placement.ec.impl";

  public static final String OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT =
      "ozone.scm.pipeline.owner.container.count";
  public static final int OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT = 3;

  // Pipeline placement policy:
  // Upper limit for how many pipelines a datanode can engage in.
  public static final String OZONE_DATANODE_PIPELINE_LIMIT =
          "ozone.scm.datanode.pipeline.limit";
  public static final int OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT = 2;

  public static final String OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS =
      "ozone.scm.datanode.disallow.same.peers";
  public static final boolean OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS_DEFAULT =
      false;

  public static final String
      OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL =
      "ozone.scm.expired.container.replica.op.scrub.interval";
  public static final String
      OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL_DEFAULT =
      "5m";

  // Upper limit for how many pipelines can be created
  // across the cluster nodes managed by SCM.
  // Only for test purpose now.
  public static final String OZONE_SCM_RATIS_PIPELINE_LIMIT =
      "ozone.scm.ratis.pipeline.limit";
  // Setting to zero by default means this limit doesn't take effect.
  public static final int OZONE_SCM_RATIS_PIPELINE_LIMIT_DEFAULT = 0;

  public static final String
      OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY =
      "ozone.scm.keyvalue.container.deletion-choosing.policy";

  public static final String OZONE_SCM_PIPELINE_PER_METADATA_VOLUME =
      "ozone.scm.pipeline.per.metadata.disk";

  public static final int OZONE_SCM_PIPELINE_PER_METADATA_VOLUME_DEFAULT = 2;

  public static final String OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN =
      "ozone.scm.datanode.ratis.volume.free-space.min";

  public static final String
      OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN_DEFAULT = "1GB";

  // Max timeout for pipeline to stay at ALLOCATED state before scrubbed.
  public static final String OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT =
      "ozone.scm.pipeline.allocated.timeout";

  public static final String OZONE_SCM_PIPELINE_LEADER_CHOOSING_POLICY =
      "ozone.scm.pipeline.leader-choose.policy";

  public static final String OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT_DEFAULT =
      "5m";

  public static final String OZONE_SCM_PIPELINE_DESTROY_TIMEOUT =
      "ozone.scm.pipeline.destroy.timeout";

  // We wait for 150s before closing containers
  // OzoneConfigKeys#OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION.
  // So, we are waiting for another 150s before deleting the pipeline
  // (150 + 150) = 300s
  public static final String OZONE_SCM_PIPELINE_DESTROY_TIMEOUT_DEFAULT =
      "300s";

  public static final String OZONE_SCM_PIPELINE_CREATION_INTERVAL =
      "ozone.scm.pipeline.creation.interval";
  public static final String OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT =
      "120s";

  public static final String OZONE_SCM_PIPELINE_SCRUB_INTERVAL =
      "ozone.scm.pipeline.scrub.interval";
  public static final String OZONE_SCM_PIPELINE_SCRUB_INTERVAL_DEFAULT =
      "150s";

  public static final String OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE =
      "ozone.scm.pipeline.creation.storage-type-aware.enabled";
  public static final boolean
      OZONE_SCM_PIPELINE_CREATION_STORAGE_TYPE_AWARE_DEFAULT = false;

  // Allow SCM to auto create factor ONE ratis pipeline.
  public static final String OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE =
      "ozone.scm.pipeline.creation.auto.factor.one";

  public static final boolean
      OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE_DEFAULT = true;

  public static final String OZONE_SCM_BLOCK_DELETION_PER_DN_DISTRIBUTION_FACTOR =
      "ozone.scm.block.deletion.per.dn.distribution.factor";

  public static final int OZONE_SCM_BLOCK_DELETION_PER_DN_DISTRIBUTION_FACTOR_DEFAULT = 8;

  public static final String OZONE_SCM_SEQUENCE_ID_BATCH_SIZE =
      "ozone.scm.sequence.id.batch.size";
  public static final int OZONE_SCM_SEQUENCE_ID_BATCH_SIZE_DEFAULT = 1000;

  // Network topology
  public static final String OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE =
      "ozone.scm.network.topology.schema.file";
  public static final String OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_DEFAULT =
      "network-topology-default.xml";

  public static final String HDDS_TRACING_ENABLED = "hdds.tracing.enabled";
  public static final boolean HDDS_TRACING_ENABLED_DEFAULT = false;

  public static final String OZONE_SCM_RATIS_PORT_KEY
      = "ozone.scm.ratis.port";
  public static final int OZONE_SCM_RATIS_PORT_DEFAULT
      = 9894;
  public static final String OZONE_SCM_GRPC_PORT_KEY
      = "ozone.scm.grpc.port";
  public static final int OZONE_SCM_GRPC_PORT_DEFAULT
      = 9895;

  public static final String OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL =
      "ozone.scm.datanode.admin.monitor.interval";
  public static final String OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT =
      "30s";
  public static final String OZONE_SCM_DATANODE_ADMIN_MONITOR_LOGGING_LIMIT =
      "ozone.scm.datanode.admin.monitor.logging.limit";
  public static final int
      OZONE_SCM_DATANODE_ADMIN_MONITOR_LOGGING_LIMIT_DEFAULT = 1000;

  public static final String OZONE_SCM_INFO_WAIT_DURATION =
      "ozone.scm.info.wait.duration";
  public static final long OZONE_SCM_INFO_WAIT_DURATION_DEFAULT =
      10 * 60;

  public static final String OZONE_SCM_CA_LIST_RETRY_INTERVAL =
      "ozone.scm.ca.list.retry.interval";
  public static final long OZONE_SCM_CA_LIST_RETRY_INTERVAL_DEFAULT = 10;

  public static final String OZONE_SCM_EVENT_PREFIX = "ozone.scm.event.";

  public static final String OZONE_SCM_EVENT_CONTAINER_REPORT_THREAD_POOL_SIZE =
      OZONE_SCM_EVENT_PREFIX + "ContainerReport.thread.pool.size";
  public static final int OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT = 10;
  /**
  SCM Event Report queue default queue wait time in millisec, i.e. 1 minute.
   */
  public static final int OZONE_SCM_EVENT_REPORT_QUEUE_WAIT_THRESHOLD_DEFAULT
      = 60000;
  /**
  SCM Event Report queue execution time wait in millisec, i.e. 2 minute.
   */
  public static final int OZONE_SCM_EVENT_REPORT_EXEC_WAIT_THRESHOLD_DEFAULT
      = 120000;
  public static final int OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT 
      = 100000;

  public static final String OZONE_SCM_HA_RATIS_RPC_TYPE =
          "ozone.scm.ha.ratis.rpc.type";
  public static final String OZONE_SCM_HA_RATIS_RPC_TYPE_DEFAULT =
          "GRPC";

  public static final String OZONE_SCM_HA_RAFT_SEGMENT_SIZE =
          "ozone.scm.ha.ratis.segment.size";
  public static final String OZONE_SCM_HA_RAFT_SEGMENT_SIZE_DEFAULT = "64MB";

  public static final String OZONE_SCM_HA_RAFT_SEGMENT_PRE_ALLOCATED_SIZE =
          "ozone.scm.ha.ratis.segment.preallocated.size";
  public static final String
          OZONE_SCM_HA_RAFT_SEGMENT_PRE_ALLOCATED_SIZE_DEFAULT = "4MB";

  public static final String OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_NUM =
      "ozone.scm.ha.ratis.log.appender.queue.num-elements";
  public static final int
      OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_NUM_DEFAULT = 1024;

  public static final String OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT =
          "ozone.scm.ha.ratis.log.appender.queue.byte-limit";
  public static final String
          OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB";

  public static final String OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL =
          "ozone.scm.ha.grpc.deadline.interval";
  public static final long
          OZONE_SCM_HA_GRPC_DEADLINE_INTERVAL_DEFAULT = 30 * 60 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_NODE_FAILURE_TIMEOUT =
          "ozone.scm.ha.ratis.server.failure.timeout.duration";
  public static final long
          OZONE_SCM_HA_RATIS_NODE_FAILURE_TIMEOUT_DEFAULT = 120 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_LEADER_READY_CHECK_INTERVAL =
          "ozone.scm.ha.ratis.leader.ready.check.interval";
  public static final long
          OZONE_SCM_HA_RATIS_LEADER_READY_CHECK_INTERVAL_DEFAULT = 2 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_LEADER_READY_WAIT_TIMEOUT =
          "ozone.scm.ha.ratis.leader.ready.wait.timeout";
  public static final long
          OZONE_SCM_HA_RATIS_LEADER_READY_WAIT_TIMEOUT_DEFAULT = 60 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_RETRY_CACHE_TIMEOUT =
          "ozone.scm.ha.ratis.server.retry.cache.timeout";
  public static final long
          OZONE_SCM_HA_RATIS_RETRY_CACHE_TIMEOUT_DEFAULT = 60 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_STORAGE_DIR =
          "ozone.scm.ha.ratis.storage.dir";

  public static final String OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED =
          "ozone.scm.ha.ratis.log.purge.enabled";
  public static final boolean OZONE_SCM_HA_RAFT_LOG_PURGE_ENABLED_DEFAULT =
          false;

  public static final String OZONE_SCM_HA_RAFT_LOG_PURGE_GAP =
          "ozone.scm.ha.ratis.log.purge.gap";
  public static final int OZONE_SCM_HA_RAFT_LOG_PURGE_GAP_DEFAULT = 1000000;

  /**
   * the config will transfer value to ratis config
   * raft.server.snapshot.auto.trigger.threshold.
   */
  public static final String OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD =
          "ozone.scm.ha.ratis.snapshot.threshold";
  public static final long OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD_DEFAULT =
          1000L;

  /**
   * the config will transfer value to ratis config
   * raft.server.snapshot.creation.gap, used by ratis to take snapshot
   * when manual trigger using api.
   */
  public static final String OZONE_SCM_HA_RATIS_SNAPSHOT_GAP
      = "ozone.scm.ha.ratis.server.snapshot.creation.gap";
  public static final long OZONE_SCM_HA_RATIS_SNAPSHOT_GAP_DEFAULT =
      1024L;
  public static final String OZONE_SCM_HA_RATIS_SNAPSHOT_DIR =
          "ozone.scm.ha.ratis.snapshot.dir";

  public static final String OZONE_SCM_HA_RATIS_LEADER_ELECTION_TIMEOUT =
          "ozone.scm.ha.ratis.leader.election.timeout";
  public static final long
          OZONE_SCM_HA_RATIS_LEADER_ELECTION_TIMEOUT_DEFAULT = 5 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_REQUEST_TIMEOUT =
          "ozone.scm.ha.ratis.request.timeout";
  public static final long
          OZONE_SCM_HA_RATIS_REQUEST_TIMEOUT_DEFAULT = 30 * 1000L;

  public static final String OZONE_SCM_HA_RATIS_SERVER_ELECTION_PRE_VOTE =
      "ozone.scm.ha.ratis.server.leaderelection.pre-vote";
  public static final boolean
      OZONE_SCM_HA_RATIS_SERVER_ELECTION_PRE_VOTE_DEFAULT = true;

  public static final String OZONE_AUDIT_LOG_DEBUG_CMD_LIST_SCMAUDIT =
      "ozone.audit.log.debug.cmd.list.scmaudit";

  public static final String OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL =
      "ozone.scm.ha.dbtransactionbuffer.flush.interval";
  public static final long
      OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL_DEFAULT = 60 * 1000L;

  public static final String NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
      "net.topology.node.switch.mapping.impl";
  public static final String HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL
      = "hdds.container.ratis.statemachine.write.wait.interval";
  public static final long HDDS_CONTAINER_RATIS_STATEMACHINE_WRITE_WAIT_INTERVAL_NS_DEFAULT = 10 * 60 * 1000_000_000L;

  public static final String OZONE_SCM_HA_RATIS_SERVER_RPC_FIRST_ELECTION_TIMEOUT
      = "ozone.scm.ha.raft.server.rpc.first-election.timeout";

  /**
   * Never constructed.
   */
  private ScmConfigKeys() {

  }
}
