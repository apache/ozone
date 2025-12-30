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

package org.apache.hadoop.ozone.om;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ratis.util.TimeDuration;

/**
 * Ozone Manager Constants.
 */
public final class OMConfigKeys {
  public static final String OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY =
      "ozone.filesystem.snapshot.enabled";
  public static final boolean OZONE_FILESYSTEM_SNAPSHOT_ENABLED_DEFAULT = true;

  // Location where the OM stores its DB files. In the future we may support
  // multiple entries for performance (sharding)..
  public static final String OZONE_OM_DB_DIRS = "ozone.om.db.dirs";

  // SCM DB directory permission
  public static final String OZONE_OM_DB_DIRS_PERMISSIONS =
      "ozone.om.db.dirs.permissions";

  public static final String OZONE_OM_HANDLER_COUNT_KEY =
      "ozone.om.handler.count.key";
  public static final int OZONE_OM_HANDLER_COUNT_DEFAULT = 100;
  public static final String OZONE_OM_READ_THREADPOOL_KEY =
      "ozone.om.read.threadpool";
  public static final int OZONE_OM_READ_THREADPOOL_DEFAULT = 10;

  public static final String OZONE_OM_DB_MAX_OPEN_FILES
      = "ozone.om.db.max.open.files";
  public static final int OZONE_OM_DB_MAX_OPEN_FILES_DEFAULT
      = -1;

  public static final String OZONE_OM_INTERNAL_SERVICE_ID =
      "ozone.om.internal.service.id";

  public static final String OZONE_OM_SERVICE_IDS_KEY =
      "ozone.om.service.ids";
  public static final String OZONE_OM_NODES_KEY =
      "ozone.om.nodes";
  public static final String OZONE_OM_NODE_ID_KEY =
      "ozone.om.node.id";
  public static final String OZONE_OM_DECOMMISSIONED_NODES_KEY =
      "ozone.om.decommissioned.nodes";
  public static final String OZONE_OM_LISTENER_NODES_KEY =
      "ozone.om.listener.nodes";

  public static final String OZONE_OM_FEATURES_DISABLED =
      "ozone.om.features.disabled";

  public static final String OZONE_OM_ADDRESS_KEY =
      "ozone.om.address";
  public static final String OZONE_OM_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_OM_PORT_DEFAULT = 9862;
  public static final String OZONE_OM_GRPC_PORT_KEY =
      "ozone.om.grpc.port";

  public static final String OZONE_OM_GRPC_BOSSGROUP_SIZE_KEY =
      "ozone.om.grpc.bossgroup.size";
  public static final int OZONE_OM_GRPC_BOSSGROUP_SIZE_DEFAULT = 8;

  public static final String OZONE_OM_GRPC_WORKERGROUP_SIZE_KEY =
      "ozone.om.grpc.workergroup.size";
  public static final int OZONE_OM_GRPC_WORKERGROUP_SIZE_DEFAULT = 32;

  public static final String OZONE_OM_GRPC_READ_THREAD_NUM_KEY =
      "ozone.om.grpc.read.thread.num";
  public static final int OZONE_OM_GRPC_READ_THREAD_NUM_DEFAULT = 32;

  public static final String OZONE_OM_HTTP_ENABLED_KEY =
      "ozone.om.http.enabled";
  public static final String OZONE_OM_HTTP_BIND_HOST_KEY =
      "ozone.om.http-bind-host";
  public static final String OZONE_OM_HTTPS_BIND_HOST_KEY =
      "ozone.om.https-bind-host";
  public static final String OZONE_OM_HTTP_ADDRESS_KEY =
      "ozone.om.http-address";
  public static final String OZONE_OM_HTTPS_ADDRESS_KEY =
      "ozone.om.https-address";
  public static final String OZONE_OM_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_OM_HTTP_BIND_PORT_DEFAULT = 9874;
  public static final int OZONE_OM_HTTPS_BIND_PORT_DEFAULT = 9875;

  public static final String OZONE_KEY_DELETING_LIMIT_PER_TASK =
      "ozone.key.deleting.limit.per.task";
  public static final int OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT = 50000;
  public static final String OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK =
      "ozone.snapshot.key.deleting.limit.per.task";
  public static final int OZONE_SNAPSHOT_KEY_DELETING_LIMIT_PER_TASK_DEFAULT
      = 20000;

  public static final String OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL =
      "ozone.om.open.key.cleanup.service.interval";
  public static final String
      OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL_DEFAULT = "24h";

  public static final String OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT =
      "ozone.om.open.key.cleanup.service.timeout";
  public static final String OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_TIMEOUT_DEFAULT
      = "300s";

  public static final String OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD =
      "ozone.om.open.key.expire.threshold";
  public static final String OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT =
      "7d";

  public static final String OZONE_OM_LEASE_HARD_LIMIT =
      "ozone.om.lease.hard.limit";
  public static final String OZONE_OM_LEASE_HARD_LIMIT_DEFAULT =
      "7d";

  public static final String OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK =
      "ozone.om.open.key.cleanup.limit.per.task";
  public static final int OZONE_OM_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT =
      1000;

  public static final String OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL =
      "ozone.om.open.mpu.cleanup.service.interval";
  public static final String
      OZONE_OM_MPU_CLEANUP_SERVICE_INTERVAL_DEFAULT = "24h";

  public static final String OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT =
      "ozone.om.open.mpu.cleanup.service.timeout";
  public static final String OZONE_OM_MPU_CLEANUP_SERVICE_TIMEOUT_DEFAULT
      = "300s";

  public static final String OZONE_OM_MPU_EXPIRE_THRESHOLD =
      "ozone.om.open.mpu.expire.threshold";
  public static final String OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT =
      "30d";

  public static final String OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK =
      "ozone.om.open.mpu.parts.cleanup.limit.per.task";
  public static final int OZONE_OM_MPU_PARTS_CLEANUP_LIMIT_PER_TASK_DEFAULT =
      1000;

  public static final String OZONE_OM_METRICS_SAVE_INTERVAL =
      "ozone.om.save.metrics.interval";
  public static final String OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT = "5m";
  public static final String OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED =
      "ozone.om.snapshot.rocksdb.metrics.enabled";
  public static final boolean
      OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED_DEFAULT = false;

  /**
   * Properties for Key/Object Lifecycle feature.
   */
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL =
      "ozone.lifecycle.service.interval";
  public static final String
      OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL_DEFAULT = "24h";
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_TIMEOUT =
      "ozone.lifecycle.service.timeout";
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_TIMEOUT_DEFAULT
      = "2h";
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_WORKERS =
      "ozone.lifecycle.service.workers";
  public static final int OZONE_KEY_LIFECYCLE_SERVICE_WORKERS_DEFAULT
      = 5;
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_ENABLED =
      "ozone.lifecycle.service.enabled";
  public static final boolean OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT = false;
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE =
      "ozone.lifecycle.service.delete.batch-size";
  public static final int OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE_DEFAULT = 1000;
  public static final String OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT =
      "ozone.lifecycle.service.delete.cached.directory.max-count";
  public static final long OZONE_KEY_LIFECYCLE_SERVICE_DELETE_CACHED_DIRECTORY_MAX_COUNT_DEFAULT = 1000000;

  /**
   * OM Ratis related configurations.
   */
  public static final String OZONE_OM_RATIS_PORT_KEY
      = "ozone.om.ratis.port";
  public static final int OZONE_OM_RATIS_PORT_DEFAULT
      = 9872;
  public static final String OZONE_OM_RATIS_RPC_TYPE_KEY
      = "ozone.om.ratis.rpc.type";
  public static final String OZONE_OM_RATIS_RPC_TYPE_DEFAULT
      = "GRPC";

  // OM Ratis Log configurations
  public static final String OZONE_OM_RATIS_STORAGE_DIR
      = "ozone.om.ratis.storage.dir";
  public static final String OZONE_OM_RATIS_SEGMENT_SIZE_KEY
      = "ozone.om.ratis.segment.size";
  public static final String OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT
      = "64MB";
  public static final String OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY
      = "ozone.om.ratis.segment.preallocated.size";
  public static final String OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT
      = "4MB";

  // OM Ratis Log Appender configurations
  public static final String
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS =
      "ozone.om.ratis.log.appender.queue.num-elements";
  public static final int
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT = 1024;
  public static final String OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT =
      "ozone.om.ratis.log.appender.queue.byte-limit";
  public static final String
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB";

  // OM Ratis Pending Write configurations
  public static final String OZONE_OM_RATIS_PENDING_WRITE_ELEMENT_LIMIT =
      "ozone.om.ratis.server.pending.write.element-limit";
  public static final int OZONE_OM_RATIS_PENDING_WRITE_NUM_LIMIT_DEFAULT = 4096;

  public static final String OZONE_OM_RATIS_LOG_PURGE_GAP =
      "ozone.om.ratis.log.purge.gap";
  public static final int OZONE_OM_RATIS_LOG_PURGE_GAP_DEFAULT = 1000000;
  public static final String OZONE_OM_RATIS_LOG_PURGE_UPTO_SNAPSHOT_INDEX
      = "ozone.om.ratis.log.purge.upto.snapshot.index";
  public static final boolean
      OZONE_OM_RATIS_LOG_PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT = true;
  public static final String
      OZONE_OM_RATIS_LOG_PURGE_PRESERVATION_LOG_NUM
      = "ozone.om.ratis.log.purge.preservation.log.num";
  public static final long
      OZONE_OM_RATIS_LOG_PURGE_PRESERVATION_LOG_NUM_DEFAULT = 0L;

  public static final String OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY
      = "ozone.om.ratis.snapshot.auto.trigger.threshold";
  public static final long
      OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000;

  public static final String OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_KEY
      = "ozone.om.ratis.snapshot.max.total.sst.size";
  public static final long
      OZONE_OM_RATIS_SNAPSHOT_MAX_TOTAL_SST_SIZE_DEFAULT = 100_000_000;

  // OM Ratis server configurations
  public static final String OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY
      = "ozone.om.ratis.server.request.timeout";
  public static final TimeDuration
      OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
  public static final String
      OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY
      = "ozone.om.ratis.server.retry.cache.timeout";
  public static final TimeDuration
      OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
  public static final String OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY
      = "ozone.om.ratis.minimum.timeout";
  public static final TimeDuration OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(5, TimeUnit.SECONDS);

  public static final String OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY
      = "ozone.om.ratis.server.failure.timeout.duration";
  public static final TimeDuration
      OZONE_OM_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT
      = TimeDuration.valueOf(120, TimeUnit.SECONDS);

  public static final String OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE =
      "ozone.om.ratis.server.leaderelection.pre-vote";
  public static final boolean
      OZONE_OM_RATIS_SERVER_ELECTION_PRE_VOTE_DEFAULT = true;

  public static final String OZONE_OM_RATIS_SERVER_CLOSE_THRESHOLD_KEY =
      "ozone.om.ratis.server.close.threshold";
  public static final TimeDuration OZONE_OM_RATIS_SERVER_CLOSE_THRESHOLD_DEFAULT =
      TimeDuration.valueOf(60, TimeUnit.SECONDS);

  // OM SnapshotProvider configurations
  public static final String OZONE_OM_RATIS_SNAPSHOT_DIR =
      "ozone.om.ratis.snapshot.dir";
  public static final String OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_KEY =
      "ozone.om.snapshot.provider.socket.timeout";
  public static final TimeDuration
      OZONE_OM_SNAPSHOT_PROVIDER_SOCKET_TIMEOUT_DEFAULT =
      TimeDuration.valueOf(5000, TimeUnit.MILLISECONDS);

  public static final String OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_KEY =
      "ozone.om.snapshot.provider.connection.timeout";
  public static final TimeDuration
      OZONE_OM_SNAPSHOT_PROVIDER_CONNECTION_TIMEOUT_DEFAULT =
      TimeDuration.valueOf(5000, TimeUnit.MILLISECONDS);

  public static final String OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_KEY =
      "ozone.om.snapshot.provider.request.timeout";
  public static final TimeDuration
      OZONE_OM_SNAPSHOT_PROVIDER_REQUEST_TIMEOUT_DEFAULT =
      TimeDuration.valueOf(300000, TimeUnit.MILLISECONDS);

  public static final String OZONE_OM_FS_SNAPSHOT_MAX_LIMIT =
      "ozone.om.fs.snapshot.max.limit";
  public static final int OZONE_OM_FS_SNAPSHOT_MAX_LIMIT_DEFAULT = 10000;

  public static final String OZONE_OM_KERBEROS_KEYTAB_FILE_KEY = "ozone.om."
      + "kerberos.keytab.file";
  public static final String OZONE_OM_KERBEROS_PRINCIPAL_KEY = "ozone.om"
      + ".kerberos.principal";
  public static final String OZONE_OM_KERBEROS_PRINCIPAL_PATTERN_KEY =
      "ozone.om.kerberos.principal.pattern";
  public static final String OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE =
      "ozone.om.http.auth.kerberos.keytab";
  public static final String OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY
      = "ozone.om.http.auth.kerberos.principal";
  public static final String OZONE_OM_HTTP_AUTH_TYPE =
      "ozone.om.http.auth.type";
  public static final String OZONE_OM_HTTP_AUTH_CONFIG_PREFIX =
      "ozone.om.http.auth.";

  // Delegation token related keys
  public static final String  DELEGATION_REMOVER_SCAN_INTERVAL_KEY =
      "ozone.manager.delegation.remover.scan.interval";
  public static final long    DELEGATION_REMOVER_SCAN_INTERVAL_DEFAULT =
      60 * 60 * 1000;
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
      "ozone.manager.delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000;  // 1 day = 86400000 ms
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY =
      "ozone.manager.delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
      7 * 24 * 60 * 60 * 1000; // 7 days

  // Comma separated acls (users, groups) allowing clients accessing
  // OM client protocol
  // when hadoop.security.authorization is true, this needs to be set in
  // hadoop-policy.xml, "*" allows all users/groups to access.
  public static final String OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL =
      "ozone.om.security.client.protocol.acl";

  // Comma separated acls (users, groups) allowing clients accessing
  // OM admin protocol.
  // When hadoop.security.authorization is true, this needs to be set in
  // hadoop-policy.xml, "*" allows all users/groups to access.
  public static final String OZONE_OM_SECURITY_ADMIN_PROTOCOL_ACL =
      "ozone.om.security.admin.protocol.acl";

  @Deprecated
  public static final String OZONE_OM_ENABLE_FILESYSTEM_PATHS =
      OmConfig.Keys.ENABLE_FILESYSTEM_PATHS;
  @Deprecated
  public static final boolean OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT =
      OmConfig.Defaults.ENABLE_FILESYSTEM_PATHS;

  public static final String OZONE_SERVER_DEFAULT_REPLICATION_KEY =
      "ozone.server.default.replication";
  public static final String OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT =
      ReplicationFactor.THREE.toString();

  public static final String OZONE_SERVER_DEFAULT_REPLICATION_TYPE_KEY =
      "ozone.server.default.replication.type";
  public static final String OZONE_SERVER_DEFAULT_REPLICATION_TYPE_DEFAULT =
      ReplicationType.RATIS.toString();

  public static final String OZONE_OM_HA_PREFIX = "ozone.om.ha";

  public static final String OZONE_FS_TRASH_INTERVAL_KEY =
      "ozone.fs.trash.interval";

  public static final long  OZONE_FS_TRASH_INTERVAL_DEFAULT = 0;

  public static final String OZONE_FS_TRASH_CHECKPOINT_INTERVAL_KEY =
      "ozone.fs.trash.checkpoint.interval";

  public static final long  OZONE_FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT = 0;

//  This property is used to define the metadata layout of file system
//  paths. If it is configured as PREFIX in combination with
//  ozone.om.enable.filesystem.paths to true then this allows to perform
//  atomic rename and delete of any directory at any level in the namespace.
//  Defaulting to SIMPLE. Supported values: SIMPLE and PREFIX.

  // Default bucket layout used by Ozone Manager during bucket creation
  // when a client does not specify the bucket layout option.
  public static final String OZONE_DEFAULT_BUCKET_LAYOUT =
      "ozone.default.bucket.layout";
  public static final String OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT =
      BucketLayout.FILE_SYSTEM_OPTIMIZED.name();
  public static final String OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED =
      BucketLayout.FILE_SYSTEM_OPTIMIZED.name();
  public static final String OZONE_BUCKET_LAYOUT_OBJECT_STORE =
      BucketLayout.OBJECT_STORE.name();

  /**
   * Configuration properties for Directory Deleting Service.
   */
  public static final String OZONE_DIR_DELETING_SERVICE_INTERVAL =
      "ozone.directory.deleting.service.interval";
  public static final String OZONE_DIR_DELETING_SERVICE_INTERVAL_DEFAULT
      = "60s";

  public static final String OZONE_PATH_DELETING_LIMIT_PER_TASK =
      "ozone.path.deleting.limit.per.task";
  // default is 20000 taking account of 32MB buffer size
  public static final int OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT = 20000;

  /**
   * Configuration properties for Snapshot Directory Service.
   */
  public static final String OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED = "ozone.snapshot.deep.cleaning.enabled";
  public static final boolean OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED_DEFAULT = false;
  /**
   * DirectoryDeepCleaning snapshots have been moved from SnapshotDirectoryCleaningService to DirectoryDeletingService.
   * Configs related to SnapshotDirectoryCleaningService are deprecated as this won't be used anywhere.
   */
  @Deprecated
  public static final String OZONE_SNAPSHOT_DIRECTORY_SERVICE_INTERVAL =
      "ozone.snapshot.directory.service.interval";
  @Deprecated
  public static final String OZONE_SNAPSHOT_DIRECTORY_SERVICE_INTERVAL_DEFAULT
      = "24h";
  @Deprecated
  public static final String OZONE_SNAPSHOT_DIRECTORY_SERVICE_TIMEOUT =
      "ozone.snapshot.directory.service.timeout";
  @Deprecated
  public static final String
      OZONE_SNAPSHOT_DIRECTORY_SERVICE_TIMEOUT_DEFAULT = "300s";

  public static final String OZONE_THREAD_NUMBER_DIR_DELETION =
      "ozone.thread.number.dir.deletion";

  public static final String OZONE_THREAD_NUMBER_KEY_DELETION =
      "ozone.thread.number.key.deletion";

  public static final int OZONE_THREAD_NUMBER_DIR_DELETION_DEFAULT = 10;

  public static final int OZONE_THREAD_NUMBER_KEY_DELETION_DEFAULT = 10;

  public static final String SNAPSHOT_SST_DELETING_LIMIT_PER_TASK =
      "ozone.snapshot.filtering.limit.per.task";
  public static final int SNAPSHOT_SST_DELETING_LIMIT_PER_TASK_DEFAULT = 2;

  public static final String SNAPSHOT_DELETING_LIMIT_PER_TASK =
      "ozone.snapshot.deleting.limit.per.task";
  public static final int SNAPSHOT_DELETING_LIMIT_PER_TASK_DEFAULT = 10;

  // Snapshot defragmentation service configuration
  public static final String SNAPSHOT_DEFRAG_LIMIT_PER_TASK =
      "ozone.snapshot.defrag.limit.per.task";
  public static final int SNAPSHOT_DEFRAG_LIMIT_PER_TASK_DEFAULT = 1;

  public static final String OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL =
      "ozone.snapshot.filtering.service.interval";
  public static final String
      OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT = "60s";

  public static final String OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL =
      "ozone.snapshot.defrag.service.interval";
  public static final String
      OZONE_SNAPSHOT_DEFRAG_SERVICE_INTERVAL_DEFAULT = "-1";
  // TODO: Disabled by default. Do not enable by default until upgrade handling is complete.

  public static final String
      OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT =
      "ozone.om.snapshot.checkpoint.dir.creation.poll.timeout";

  public static final String
      OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT_DEFAULT = "20s";

  public static final String OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH =
      "ozone.om.grpc.maximum.response.length";
  /** Default value for GRPC_MAXIMUM_RESPONSE_LENGTH. */
  public static final int OZONE_OM_GRPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT =
      128 * 1024 * 1024;

  public static final String OZONE_OM_S3_GPRC_SERVER_ENABLED =
      "ozone.om.s3.grpc.server_enabled";
  public static final boolean OZONE_OM_S3_GRPC_SERVER_ENABLED_DEFAULT =
      true;

  public static final String OZONE_OM_NAMESPACE_STRICT_S3 =
      "ozone.om.namespace.s3.strict";
  public static final boolean OZONE_OM_NAMESPACE_STRICT_S3_DEFAULT =
      true;

  /**
   * Configuration properties for OMAdminProtcol service.
   */
  public static final String OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_KEY =
      "ozone.om.admin.protocol.max.retries";
  public static final int OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_DEFAULT = 20;
  public static final String OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_KEY =
      "ozone.om.admin.protocol.wait.between.retries";
  public static final long OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_DEFAULT
      = 1000;
  public static final String OZONE_OM_TRANSPORT_CLASS =
      "ozone.om.transport.class";
  public static final String OZONE_OM_TRANSPORT_CLASS_DEFAULT =
      "org.apache.hadoop.ozone.om.protocolPB"
          + ".Hadoop3OmTransportFactory";
  public static final String OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT =
      "ozone.om.unflushed.transaction.max.count";
  public static final int OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT_DEFAULT
      = 10000;

  /**
   * This configuration shall be enabled to utilize the functionality of the
   * fine-grained KEY_PATH_LOCK.
   */
  public static final String OZONE_OM_KEY_PATH_LOCK_ENABLED =
      "ozone.om.key.path.lock.enabled";
  public static final boolean OZONE_OM_KEY_PATH_LOCK_ENABLED_DEFAULT = false;

  public static final String OZONE_OM_MULTITENANCY_ENABLED =
      "ozone.om.multitenancy.enabled";
  public static final boolean OZONE_OM_MULTITENANCY_ENABLED_DEFAULT = false;

  /**
   * Temporary configuration properties for Ranger REST use in multitenancy.
   */
  public static final String OZONE_RANGER_OM_IGNORE_SERVER_CERT =
      "ozone.om.ranger.ignore.cert";
  public static final boolean OZONE_RANGER_OM_IGNORE_SERVER_CERT_DEFAULT =
      true;
  public static final String OZONE_RANGER_OM_CONNECTION_TIMEOUT =
      "ozone.om.ranger.connection.timeout";
  public static final String OZONE_RANGER_OM_CONNECTION_TIMEOUT_DEFAULT = "5s";
  public static final String OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT =
      "ozone.om.ranger.connection.request.timeout";
  public static final String
      OZONE_RANGER_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT = "5s";
  public static final String OZONE_OM_RANGER_HTTPS_ADMIN_API_USER =
      "ozone.om.ranger.https.admin.api.user";
  // TODO: Note this should be removed once Ranger Java Client is in place.
  //  And Ranger SPNEGO auth (ranger.spnego.kerberos.principal ?) should be used
  //  instead. Or keep this solely for dev testing. See HDDS-5836.
  public static final String OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD =
      "ozone.om.ranger.https.admin.api.passwd";
  public static final String OZONE_RANGER_HTTPS_ADDRESS_KEY =
      "ozone.om.ranger.https-address";
  public static final String OZONE_RANGER_SERVICE =
      "ozone.om.ranger.service";

  public static final String OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL
      = "ozone.om.multitenancy.ranger.sync.interval";
  public static final TimeDuration
      OZONE_OM_MULTITENANCY_RANGER_SYNC_INTERVAL_DEFAULT
      = TimeDuration.valueOf(600, TimeUnit.SECONDS);
  public static final String OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT
      = "ozone.om.multitenancy.ranger.sync.timeout";
  public static final TimeDuration
      OZONE_OM_MULTITENANCY_RANGER_SYNC_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(10, TimeUnit.SECONDS);

  public static final String OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE
      = "ozone.om.container.location.cache.size";
  public static final int OZONE_OM_CONTAINER_LOCATION_CACHE_SIZE_DEFAULT
      = 100_000;

  public static final String OZONE_OM_CONTAINER_LOCATION_CACHE_TTL
      = "ozone.om.container.location.cache.ttl";

  public static final TimeDuration OZONE_OM_CONTAINER_LOCATION_CACHE_TTL_DEFAULT
      = TimeDuration.valueOf(360, TimeUnit.MINUTES);

  public static final String OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR
      = "ozone.om.enable.ofs.shared.tmp.dir";
  public static final boolean OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR_DEFAULT
      = false;

  public static final String OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE =
      "ozone.om.snapshot.cache.max.size";
  public static final int OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT = 10;

  public static final String OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF =
      "ozone.om.snapshot.force.full.diff";

  public static final boolean OZONE_OM_SNAPSHOT_FORCE_FULL_DIFF_DEFAULT = false;

  public static final String OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS =
      "ozone.om.snapshot.diff.disable.native.libs";

  public static final boolean
      OZONE_OM_SNAPSHOT_DIFF_DISABLE_NATIVE_LIBS_DEFAULT = false;
  public static final String OZONE_OM_SNAPSHOT_DIFF_DB_DIR
      = "ozone.om.snapshot.diff.db.dir";

  public static final String OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE
      = "ozone.om.snapshot.diff.max.page.size";

  public static final String OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES
      = "ozone.om.snapshot.db.max.open.files";
  public static final int OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT
      = 100;
  public static final int OZONE_OM_SNAPSHOT_DIFF_REPORT_MAX_PAGE_SIZE_DEFAULT
      = 1000;

  public static final String OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE
      = "ozone.om.snapshot.diff.thread.pool.size";
  public static final int OZONE_OM_SNAPSHOT_DIFF_THREAD_POOL_SIZE_DEFAULT
      = 10;

  public static final String OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME
      = "ozone.om.snapshot.diff.job.default.wait.time";
  public static final long OZONE_OM_SNAPSHOT_DIFF_JOB_DEFAULT_WAIT_TIME_DEFAULT
      = TimeUnit.MINUTES.toMillis(1);

  public static final String OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK
      = "ozone.om.snapshot.diff.max.jobs.purge.per.task";
  public static final int OZONE_OM_SNAPSHOT_DIFF_MAX_JOBS_PURGE_PER_TASK_DEFAULT
      = 100;

  public static final String OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME
      = "ozone.om.snapshot.diff.job.report.persistent.time";
  public static final long
      OZONE_OM_SNAPSHOT_DIFF_JOB_REPORT_PERSISTENT_TIME_DEFAULT
      = TimeUnit.DAYS.toMillis(7);

  public static final String OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL
      = "ozone.om.snapshot.diff.cleanup.service.run.interval";
  public static final String
      OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL
      = "ozone.om.snapshot.cache.cleanup.service.run.interval";
  public static final long
      OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT
      = TimeUnit.MINUTES.toMillis(1);
  public static final long
      OZONE_OM_SNAPSHOT_CACHE_CLEANUP_SERVICE_RUN_INTERVAL_DEFAULT
      = TimeUnit.MINUTES.toMillis(1);

  public static final String OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT
      = "ozone.om.snapshot.diff.cleanup.service.timeout";

  public static final long
      OZONE_OM_SNAPSHOT_DIFF_CLEANUP_SERVICE_TIMEOUT_DEFAULT
      = TimeUnit.MINUTES.toMillis(5);

  public static final String
      OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB
      = "ozone.om.snapshot.diff.max.allowed.keys.changed.per.job";
  public static final long
      OZONE_OM_SNAPSHOT_DIFF_MAX_ALLOWED_KEYS_CHANGED_PER_DIFF_JOB_DEFAULT
      = 10_000_000;

  public static final String OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE
      = "ozone.om.upgrade.quota.recalculate.enabled";

  public static final boolean OZONE_OM_UPGRADE_QUOTA_RECALCULATE_ENABLE_DEFAULT
      = true;

  public static final String OZONE_OM_MAX_BUCKET =
      "ozone.om.max.buckets";
  public static final int OZONE_OM_MAX_BUCKET_DEFAULT = 100000;

  public static final String OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_KEY =
      "ozone.om.edekcacheloader.initial.delay.ms";

  public static final int OZONE_OM_EDEKCACHELOADER_INITIAL_DELAY_MS_DEFAULT = 3000;

  public static final String OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_KEY = "ozone.om.edekcacheloader.interval.ms";

  public static final int OZONE_OM_EDEKCACHELOADER_INTERVAL_MS_DEFAULT = 1000;

  public static final String OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_KEY =
      "ozone.om.edekcacheloader.max-retries";
  public static final int OZONE_OM_EDEKCACHELOADER_MAX_RETRIES_DEFAULT = 10;

  /**
   * Configuration properties for Compaction Service.
   */
  public static final String OZONE_OM_COMPACTION_SERVICE_ENABLED = "ozone.om.compaction.service.enabled";
  public static final boolean OZONE_OM_COMPACTION_SERVICE_ENABLED_DEFAULT = false;
  public static final String OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL =
      "ozone.om.compaction.service.run.interval";
  public static final long OZONE_OM_COMPACTION_SERVICE_RUN_INTERVAL_DEFAULT
      = TimeUnit.HOURS.toMillis(6);

  public static final String OZONE_OM_COMPACTION_SERVICE_TIMEOUT
      = "ozone.om.compaction.service.timeout";
  public static final String OZONE_OM_COMPACTION_SERVICE_TIMEOUT_DEFAULT = "10m";
  public static final String OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES
      = "ozone.om.compaction.service.columnfamilies";
  public static final String OZONE_OM_COMPACTION_SERVICE_COLUMNFAMILIES_DEFAULT =
      "keyTable,fileTable,directoryTable,deletedTable,deletedDirectoryTable,multipartInfoTable";

  /**
   * Configuration to enable/disable non-snapshot diff table compaction when snapshots are evicted from cache.
   */
  public static final String OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES = 
      "ozone.om.snapshot.compact.non.snapshot.diff.tables";
  public static final boolean OZONE_OM_SNAPSHOT_COMPACT_NON_SNAPSHOT_DIFF_TABLES_DEFAULT = false;

  public static final String OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT =
      "ozone.om.hierarchical.resource.locks.soft.limit";
  public static final int OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_SOFT_LIMIT_DEFAULT = 1024;

  public static final String OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT =
      "ozone.om.hierarchical.resource.locks.hard.limit";
  public static final int OZONE_OM_HIERARCHICAL_RESOURCE_LOCKS_HARD_LIMIT_DEFAULT = 10000;
  public static final String OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_SERVICE_INTERVAL =
      "ozone.om.snapshot.local.data.manager.service.interval";
  public static final String OZONE_OM_SNAPSHOT_LOCAL_DATA_MANAGER_SERVICE_INTERVAL_DEFAULT = "5m";

  /**
   * Never constructed.
   */
  private OMConfigKeys() {
  }
}
