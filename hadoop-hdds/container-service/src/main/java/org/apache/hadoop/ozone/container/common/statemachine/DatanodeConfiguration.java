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

package org.apache.hadoop.ozone.container.common.statemachine;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.hadoop.hdds.conf.ConfigTag.CONTAINER;
import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.MANAGEMENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.STORAGE;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONFIG_PREFIX;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class used for high level datanode configuration parameters.
 */
@ConfigGroup(prefix = CONFIG_PREFIX)
public class DatanodeConfiguration extends ReconfigurableConfig {
  public static final String CONFIG_PREFIX = "hdds.datanode";

  private static final String BLOCK_DELETE_THREAD_MAX = "block.delete.threads.max";

  public static final String HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX =
      CONFIG_PREFIX + "." + BLOCK_DELETE_THREAD_MAX;

  private static final Logger LOG = LoggerFactory.getLogger(DatanodeConfiguration.class);

  static final String CONTAINER_DELETE_THREADS_MAX_KEY = "hdds.datanode.container.delete.threads.max";
  static final String CONTAINER_CLOSE_THREADS_MAX_KEY = "hdds.datanode.container.close.threads.max";
  static final String PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY = "hdds.datanode.periodic.disk.check.interval.minutes";
  public static final String DISK_CHECK_FILE_SIZE_KEY = "hdds.datanode.disk.check.io.file.size";
  public static final String DISK_CHECK_IO_TEST_COUNT_KEY = "hdds.datanode.disk.check.io.test.count";
  public static final String DISK_CHECK_IO_FAILURES_TOLERATED_KEY = "hdds.datanode.disk.check.io.failures.tolerated";
  public static final String FAILED_DATA_VOLUMES_TOLERATED_KEY = "hdds.datanode.failed.data.volumes.tolerated";
  public static final String FAILED_METADATA_VOLUMES_TOLERATED_KEY = "hdds.datanode.failed.metadata.volumes.tolerated";
  public static final String FAILED_DB_VOLUMES_TOLERATED_KEY = "hdds.datanode.failed.db.volumes.tolerated";
  public static final String DISK_CHECK_MIN_GAP_KEY = "hdds.datanode.disk.check.min.gap";
  public static final String DISK_CHECK_TIMEOUT_KEY = "hdds.datanode.disk.check.timeout";

  // Minimum space should be left on volume.
  // Ex: If volume has 1000GB and minFreeSpace is configured as 10GB,
  // In this case when availableSpace is 10GB or below, volume is assumed as full
  public static final String HDDS_DATANODE_VOLUME_MIN_FREE_SPACE = "hdds.datanode.volume.min.free.space";
  public static final String HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT = "20GB";
  // Minimum percent of space should be left on volume.
  // Ex: If volume has 1000GB and minFreeSpacePercent is configured as 2%,
  // In this case when availableSpace is 20GB(2% of 1000) or below, volume is assumed as full
  public static final String HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT =
      "hdds.datanode.volume.min.free.space.percent";
  public static final float HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT = 0.02f;

  public static final String WAIT_ON_ALL_FOLLOWERS = "hdds.datanode.wait.on.all.followers";
  public static final String CONTAINER_SCHEMA_V3_ENABLED = "hdds.datanode.container.schema.v3.enabled";
  public static final String CONTAINER_CHECKSUM_LOCK_STRIPES_KEY = "hdds.datanode.container.checksum.lock.stripes";
  public static final String CONTAINER_CLIENT_CACHE_SIZE = "hdds.datanode.container.client.cache.size";
  public static final String CONTAINER_CLIENT_CACHE_STALE_THRESHOLD =
      "hdds.datanode.container.client.cache.stale.threshold";

  static final boolean CHUNK_DATA_VALIDATION_CHECK_DEFAULT = false;

  static final long PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT = 60;

  static final int FAILED_VOLUMES_TOLERATED_DEFAULT = -1;

  public static final int DISK_CHECK_IO_TEST_COUNT_DEFAULT = 3;

  public static final int DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT = 1;

  public static final int DISK_CHECK_FILE_SIZE_DEFAULT = 100;

  static final boolean WAIT_ON_ALL_FOLLOWERS_DEFAULT = false;

  static final Duration DISK_CHECK_MIN_GAP_DEFAULT = Duration.ofMinutes(10);

  static final Duration DISK_CHECK_TIMEOUT_DEFAULT = Duration.ofMinutes(10);

  static final boolean CONTAINER_SCHEMA_V3_ENABLED_DEFAULT = true;
  static final long ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_DEFAULT = 32 * 1024 * 1024;
  static final int ROCKSDB_LOG_MAX_FILE_NUM_DEFAULT = 64;
  // one hour
  static final long ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_DEFAULT = 1L * 60 * 60 * 1000 * 1000;
  static final int ROCKSDB_MAX_OPEN_FILES_DEFAULT = 1024;
  public static final String ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_KEY = "hdds.datanode.rocksdb.log.max-file-size";
  public static final String ROCKSDB_LOG_MAX_FILE_NUM_KEY = "hdds.datanode.rocksdb.log.max-file-num";
  public static final String ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_KEY =
      "hdds.datanode.rocksdb.delete_obsolete_files_period";
  public static final Boolean
      OZONE_DATANODE_CHECK_EMPTY_CONTAINER_DIR_ON_DELETE_DEFAULT = false;
  public static final int CONTAINER_CHECKSUM_LOCK_STRIPES_DEFAULT = 127;
  public static final int CONTAINER_CLIENT_CACHE_SIZE_DEFAULT = 100;
  public static final int
      CONTAINER_CLIENT_CACHE_STALE_THRESHOLD_MILLISECONDS_DEFAULT = 10000;

  private static final long AUTO_COMPACTION_SMALL_SST_FILE_INTERVAL_MINUTES_DEFAULT = 120;
  private static final int AUTO_COMPACTION_SMALL_SST_FILE_THREADS_DEFAULT = 1;

  static final int CONTAINER_DELETE_THREADS_DEFAULT = 2;
  static final int CONTAINER_CLOSE_THREADS_DEFAULT = 3;
  static final int BLOCK_DELETE_THREADS_DEFAULT = 5;

  public static final String BLOCK_DELETE_COMMAND_WORKER_INTERVAL =
      "hdds.datanode.block.delete.command.worker.interval";
  public static final Duration BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT = Duration.ofSeconds(2);

  /**
   * Number of threads per volume that Datanode will use for chunk read.
   */
  @Config(key = "hdds.datanode.read.chunk.threads.per.volume",
      type = ConfigType.INT,
      defaultValue = "10",
      tags = {DATANODE},
      description = "Number of threads per volume that Datanode will use for reading replicated chunks."
  )
  private int numReadThreadPerVolume = 10;

  /**
   * The maximum number of threads used to delete containers on a datanode
   * simultaneously.
   */
  @Config(key = "hdds.datanode.container.delete.threads.max",
      type = ConfigType.INT,
      defaultValue = "2",
      tags = {DATANODE},
      description = "The maximum number of threads used to delete containers on a datanode"
  )
  private int containerDeleteThreads = CONTAINER_DELETE_THREADS_DEFAULT;

  /**
   * The maximum number of threads used to close containers on a datanode
   * simultaneously.
   */
  @Config(key = "hdds.datanode.container.close.threads.max",
      type = ConfigType.INT,
      defaultValue = "3",
      tags = {DATANODE},
      description = "The maximum number of threads used to close containers on a datanode"
  )
  private int containerCloseThreads = CONTAINER_CLOSE_THREADS_DEFAULT;

  /**
   * The maximum number of threads used to handle delete block commands.
   * It takes about 200ms to open a RocksDB with HDD media, so basically DN
   * can handle 300 individual container delete tx every 60s if RocksDB cache
   * missed. With max threads 5, optimistically DN can handle 1500 individual
   * container delete tx in 60s with RocksDB cache miss.
   */
  @Config(key = "hdds.datanode.block.delete.threads.max",
      type = ConfigType.INT,
      defaultValue = "5",
      tags = {DATANODE},
      description = "The maximum number of threads used to handle delete blocks on a datanode"
  )
  private int blockDeleteThreads = BLOCK_DELETE_THREADS_DEFAULT;

  /**
   * The maximum number of commands in queued list.
   * 1440 = 60 * 24, which means if SCM send a delete command every minute,
   * if the commands are pined up for more than 1 day, DN will start to discard
   * new comming commands.
   */
  @Config(key = "hdds.datanode.block.delete.queue.limit",
      type = ConfigType.INT,
      defaultValue = "5",
      tags = {DATANODE},
      description = "The maximum number of block delete commands queued on " +
          " a datanode, This configuration is also used by the SCM to " +
          "control whether to send delete commands to the DN. If the DN" +
          " has more commands waiting in the queue than this value, " +
          "the SCM will not send any new block delete commands. until the " +
          "DN has processed some commands and the queue length is reduced."
  )
  private int blockDeleteQueueLimit = 5;

  @Config(key = "hdds.datanode.block.delete.command.worker.interval",
      type = ConfigType.TIME,
      defaultValue = "2s",
      tags = {DATANODE},
      description = "The interval between DeleteCmdWorker execution of delete commands."
  )
  private Duration blockDeleteCommandWorkerInterval =
      BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT;

  /**
   * The maximum number of commands in queued list.
   * if the commands limit crosses limit, then command will be ignored.
   */
  @Config(key = "hdds.datanode.command.queue.limit",
      type = ConfigType.INT,
      defaultValue = "5000",
      tags = {DATANODE},
      description = "The default maximum number of commands in the queue and command type's sub-queue on a datanode"
  )
  private int cmdQueueLimit = 5000;

  @Config(key = "hdds.datanode.block.deleting.service.interval",
          defaultValue = "60s",
          type = ConfigType.TIME,
          tags = { ConfigTag.SCM, ConfigTag.DELETION },
          description =
                  "Time interval of the Datanode block deleting service. The "
                          + "block deleting service runs on Datanode "
                          + "periodically and deletes blocks queued for "
                          + "deletion. Unit could be defined with "
                          + "postfix (ns,ms,s,m,h,d). "
  )
  private Duration blockDeletionInterval = Duration.ofSeconds(60);

  @Config(key = "hdds.datanode.recovering.container.scrubbing.service.interval",
      defaultValue = "1m",
      type = ConfigType.TIME,
      tags = { ConfigTag.SCM, ConfigTag.DELETION },
      description =
          "Time interval of the stale recovering container scrubbing " +
              "service. The recovering container scrubbing service runs " +
              "on Datanode periodically and deletes stale recovering " +
              "container Unit could be defined with postfix (ns,ms,s,m,h,d)."
  )
  private Duration recoveringContainerScrubInterval = Duration.ofMinutes(10);

  /**
   * The maximum time to wait for acquiring the container lock when processing
   * a delete block transaction.
   * If a timeout occurs while attempting to get the lock, the delete block
   * transaction won't be immediately discarded. Instead, it will be retried
   * after all the current delete block transactions have been processed.
   */
  @Config(key = "hdds.datanode.block.delete.max.lock.wait.timeout",
      defaultValue = "100ms",
      type = ConfigType.TIME,
      tags = { DATANODE, ConfigTag.DELETION},
      description = "Timeout for the thread used to process the delete block command to wait for the container lock."
  )
  private long blockDeleteMaxLockWaitTimeoutMs = Duration.ofMillis(100).toMillis();

  @Config(key = "hdds.datanode.block.deleting.limit.per.interval",
      defaultValue = "20000",
      reconfigurable = true,
      type = ConfigType.INT,
      tags = { ConfigTag.SCM, ConfigTag.DELETION, DATANODE },
      description = "Number of blocks to be deleted in an interval."
  )
  private int blockLimitPerInterval = 20000;

  @Config(key = "hdds.datanode.block.deleting.max.lock.holding.time",
      defaultValue = "1s",
      type = ConfigType.TIME,
      tags = { DATANODE, ConfigTag.DELETION },
      description =
          "This configuration controls the maximum time that the block "
          + "deleting service can hold the lock during the deletion of blocks. "
          + "Once this configured time period is reached, the service will "
          + "release and re-acquire the lock. This is not a hard limit as the "
          + "time check only occurs after the completion of each transaction, "
          + "which means the actual execution time may exceed this limit. "
          + "Unit could be defined with postfix (ns,ms,s,m,h,d). "
  )
  private long blockDeletingMaxLockHoldingTime = Duration.ofSeconds(1).toMillis();

  @Config(key = "hdds.datanode.volume.min.free.space",
      defaultValue = "-1",
      type = ConfigType.SIZE,
      tags = { OZONE, CONTAINER, STORAGE, MANAGEMENT },
      description = "This determines the free space to be used for closing containers" +
          " When the difference between volume capacity and used reaches this number," +
          " containers that reside on this volume will be closed and no new containers" +
          " would be allocated on this volume." +
          " Max of min.free.space and min.free.space.percent will be used as final value."
  )
  private long minFreeSpace = getDefaultFreeSpace();

  @Config(key = "hdds.datanode.volume.min.free.space.percent",
      defaultValue = "0.02", // match HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT
      type = ConfigType.FLOAT,
      tags = { OZONE, CONTAINER, STORAGE, MANAGEMENT },
      description = "This determines the free space percent to be used for closing containers" +
          " When the difference between volume capacity and used reaches (free.space.percent of volume capacity)," +
          " containers that reside on this volume will be closed and no new containers" +
          " would be allocated on this volume." +
          " Max of min.free.space or min.free.space.percent will be used as final value."
  )
  private float minFreeSpaceRatio = HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT;

  @Config(key = "hdds.datanode.periodic.disk.check.interval.minutes",
      defaultValue = "60",
      type = ConfigType.LONG,
      tags = { DATANODE },
      description = "Periodic disk check run interval in minutes."
  )
  private long periodicDiskCheckIntervalMinutes =
      PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;

  @Config(key = "hdds.datanode.failed.data.volumes.tolerated",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of data volumes that are allowed to fail "
          + "before a datanode stops offering service. "
          + "Config this to -1 means unlimited, but we should have "
          + "at least one good volume left."
  )
  private int failedDataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;

  @Config(key = "hdds.datanode.failed.metadata.volumes.tolerated",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of metadata volumes that are allowed to fail "
          + "before a datanode stops offering service. "
          + "Config this to -1 means unlimited, but we should have "
          + "at least one good volume left."
  )
  private int failedMetadataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;

  @Config(key = "hdds.datanode.failed.db.volumes.tolerated",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of db volumes that are allowed to fail "
          + "before a datanode stops offering service. "
          + "Config this to -1 means unlimited, but we should have "
          + "at least one good volume left."
  )
  private int failedDbVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;

  @Config(key = "hdds.datanode.disk.check.io.test.count",
      defaultValue = "3",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of IO tests required to determine if a disk " +
          " has failed. Each disk check does one IO test. The volume will be " +
          "failed if more than " +
          "hdds.datanode.disk.check.io.failures.tolerated out of the last " +
          "hdds.datanode.disk.check.io.test.count runs failed. Set to 0 " +
          "to disable disk IO checks."
  )
  private int volumeIOTestCount = DISK_CHECK_IO_TEST_COUNT_DEFAULT;

  @Config(key = "hdds.datanode.disk.check.io.failures.tolerated",
      defaultValue = "1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of IO tests out of the last hdds.datanode" +
          ".disk.check.io.test.count test run that are allowed to fail before" +
          " the volume is marked as failed."
  )
  private int volumeIOFailureTolerance =
      DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT;

  @Config(key = "hdds.datanode.disk.check.io.file.size",
      defaultValue = "100B",
      type = ConfigType.SIZE,
      tags = { DATANODE },
      description = "The size of the temporary file that will be synced to " +
          "the disk and " +
          "read back to assess its health. The contents of the " +
          "file will be stored in memory during the duration of the check."
  )
  private int volumeHealthCheckFileSize =
      DISK_CHECK_FILE_SIZE_DEFAULT;

  @Config(key = "hdds.datanode.disk.check.min.gap",
      defaultValue = "10m",
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "The minimum gap between two successive checks of the same"
          + " Datanode volume. Unit could be defined with"
          + " postfix (ns,ms,s,m,h,d)."
  )
  private Duration diskCheckMinGap = DISK_CHECK_MIN_GAP_DEFAULT;

  @Config(key = "hdds.datanode.disk.check.timeout",
      defaultValue = "10m",
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "Maximum allowed time for a disk check to complete."
          + " If the check does not complete within this time interval"
          + " then the disk is declared as failed. Unit could be defined with"
          + " postfix (ns,ms,s,m,h,d)."
  )
  private Duration diskCheckTimeout = DISK_CHECK_TIMEOUT_DEFAULT;

  @Config(key = "hdds.datanode.chunk.data.validation.check",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = { DATANODE },
      description = "Enable safety checks such as checksum validation for Ratis calls."
  )
  private boolean isChunkDataValidationCheck =
      CHUNK_DATA_VALIDATION_CHECK_DEFAULT;

  @Config(key = "hdds.datanode.wait.on.all.followers",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = { DATANODE },
      description = "Defines whether the leader datanode will wait for both"
          + "followers to catch up before removing the stateMachineData from "
          + "the cache."
  )

  private boolean waitOnAllFollowers = WAIT_ON_ALL_FOLLOWERS_DEFAULT;

  @Config(key = "hdds.datanode.container.schema.v3.enabled",
      defaultValue = "true",
      type = ConfigType.BOOLEAN,
      tags = { DATANODE },
      description = "Enable use of container schema v3(one rocksdb per disk)."
  )
  private boolean containerSchemaV3Enabled =
      CONTAINER_SCHEMA_V3_ENABLED_DEFAULT;

  @Config(key = "hdds.datanode.container.schema.v3.key.separator",
      defaultValue = "|",
      type = ConfigType.STRING,
      tags = { DATANODE },
      description = "The default separator between Container ID and container meta key name."
  )
  private String containerSchemaV3KeySeparator = "|";

  @Config(key = "hdds.datanode.rocksdb.log.level",
      defaultValue = "INFO",
      type = ConfigType.STRING,
      tags = { DATANODE },
      description = "The user log level of RocksDB(DEBUG/INFO/WARN/ERROR/FATAL))"
  )
  private String rocksdbLogLevel = "INFO";

  @Config(key = "hdds.datanode.rocksdb.log.max-file-size",
      defaultValue = "32MB",
      type = ConfigType.SIZE,
      tags = { DATANODE },
      description = "The max size of each user log file of RocksDB. O means no size limit."
  )
  private long rocksdbLogMaxFileSize = ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_DEFAULT;

  @Config(key = "hdds.datanode.rocksdb.log.max-file-num",
      defaultValue = "64",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The max user log file number to keep for each RocksDB"
  )
  private int rocksdbLogMaxFileNum = ROCKSDB_LOG_MAX_FILE_NUM_DEFAULT;

  /**
   * Following RocksDB related configuration applies to Schema V3 only.
   */
  @Config(key = "hdds.datanode.rocksdb.delete-obsolete-files-period",
      defaultValue = "1h", timeUnit = MICROSECONDS,
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "Periodicity when obsolete files get deleted. Default is 1h."
  )
  private long rocksdbDeleteObsoleteFilesPeriod =
      ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_DEFAULT;

  @Config(key = "hdds.datanode.rocksdb.max-open-files",
      defaultValue = "1024",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The total number of files that a RocksDB can open. "
  )
  private int rocksdbMaxOpenFiles = ROCKSDB_MAX_OPEN_FILES_DEFAULT;

  @Config(key = "hdds.datanode.rocksdb.auto-compaction-small-sst-file",
      defaultValue = "true",
      type = ConfigType.BOOLEAN,
      tags = { DATANODE },
      description = "Auto compact small SST files " +
          "(rocksdb.auto-compaction-small-sst-file-size-threshold) when " +
          "count exceeds (rocksdb.auto-compaction-small-sst-file-num-threshold)"
  )
  private boolean autoCompactionSmallSstFile = true;

  @Config(key = "hdds.datanode.rocksdb.auto-compaction-small-sst-file-size-threshold",
      defaultValue = "1MB",
      type = ConfigType.SIZE,
      tags = { DATANODE },
      description = "SST files smaller than this configuration will be auto compacted."
  )
  private long autoCompactionSmallSstFileSize = 1024 * 1024;

  @Config(key = "hdds.datanode.rocksdb.auto-compaction-small-sst-file-num-threshold",
      defaultValue = "512",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "Auto compaction will happen if the number of small SST files exceeds this threshold."
  )
  private int autoCompactionSmallSstFileNum = 512;

  @Config(key = "hdds.datanode.rocksdb.auto-compaction-small-sst-file.interval.minutes",
      defaultValue = "120",
      type = ConfigType.LONG,
      tags = { DATANODE },
      description = "Auto compact small SST files interval in minutes."
  )
  private long autoCompactionSmallSstFileIntervalMinutes =
      AUTO_COMPACTION_SMALL_SST_FILE_INTERVAL_MINUTES_DEFAULT;

  @Config(key = "hdds.datanode.rocksdb.auto-compaction-small-sst-file.threads",
      defaultValue = "1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "Auto compact small SST files threads."
  )
  private int autoCompactionSmallSstFileThreads =
      AUTO_COMPACTION_SMALL_SST_FILE_THREADS_DEFAULT;

  /**
   * Whether to check container directory or not to determine
   * container is empty.
   */
  @Config(key = "hdds.datanode.check.empty.container.dir.on.delete",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = { DATANODE },
      description = "Boolean Flag to decide whether to check container directory or not to determine container is empty"
  )
  private boolean bCheckEmptyContainerDir =
      OZONE_DATANODE_CHECK_EMPTY_CONTAINER_DIR_ON_DELETE_DEFAULT;

  /**
   * Whether to check container directory or not to determine
   * container is empty.
   */
  @Config(key = "hdds.datanode.container.checksum.lock.stripes",
      type = ConfigType.INT,
      defaultValue = "127",
      tags = { DATANODE },
      description = "The number of lock stripes used to coordinate modifications to container checksum information. " +
          "This information is only updated after a container is closed and does not affect the data read or write" +
          " path. Each container in the datanode will be mapped to one lock which will only be held while its " +
          "checksum information is updated."
  )
  private int containerChecksumLockStripes = CONTAINER_CHECKSUM_LOCK_STRIPES_DEFAULT;

  @Config(key = "hdds.datanode.container.client.cache.size",
      type = ConfigType.INT,
      defaultValue = "100",
      tags = { DATANODE },
      description = "The maximum number of clients to be cached by the datanode client manager"
  )
  private int containerClientCacheSize = CONTAINER_CLIENT_CACHE_SIZE_DEFAULT;

  @Config(key = "hdds.datanode.container.client.cache.stale.threshold",
      type = ConfigType.INT,
      defaultValue = "10000",
      tags = { DATANODE },
      description = "The stale threshold in ms for a client in cache. After this threshold the client " +
          "is evicted from cache."
  )
  private int containerClientCacheStaleThreshold =
      CONTAINER_CLIENT_CACHE_STALE_THRESHOLD_MILLISECONDS_DEFAULT;

  @Config(key = "hdds.datanode.delete.container.timeout",
      type = ConfigType.TIME,
      defaultValue = "60s",
      tags = { DATANODE },
      description = "If a delete container request spends more than this time waiting on the container lock or " +
          "performing pre checks, the command will be skipped and SCM will resend it automatically. This avoids " +
          "commands running for a very long time without SCM being informed of the progress."
  )
  private long deleteContainerTimeoutMs = Duration.ofSeconds(60).toMillis();

  @SuppressWarnings("checkstyle:MethodLength")
  @PostConstruct
  public void validate() {
    if (containerDeleteThreads < 1) {
      LOG.warn(CONTAINER_DELETE_THREADS_MAX_KEY + " must be greater than zero" +
              " and was set to {}. Defaulting to {}",
          containerDeleteThreads, CONTAINER_DELETE_THREADS_DEFAULT);
      containerDeleteThreads = CONTAINER_DELETE_THREADS_DEFAULT;
    }

    if (containerCloseThreads < 1) {
      LOG.warn(CONTAINER_CLOSE_THREADS_MAX_KEY + " must be greater than zero" +
              " and was set to {}. Defaulting to {}",
          containerCloseThreads, CONTAINER_CLOSE_THREADS_DEFAULT);
      containerCloseThreads = CONTAINER_CLOSE_THREADS_DEFAULT;
    }

    if (periodicDiskCheckIntervalMinutes == 0) {
      LOG.warn("{} must not be zero. Defaulting to {}",
          PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY,
          PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT);
      periodicDiskCheckIntervalMinutes = PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;
    }

    if (failedDataVolumesTolerated < -1) {
      LOG.warn(FAILED_DATA_VOLUMES_TOLERATED_KEY +
          "must be greater than -1 and was set to {}. Defaulting to {}",
          failedDataVolumesTolerated, FAILED_VOLUMES_TOLERATED_DEFAULT);
      failedDataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;
    }

    if (failedMetadataVolumesTolerated < -1) {
      LOG.warn(FAILED_METADATA_VOLUMES_TOLERATED_KEY +
              "must be greater than -1 and was set to {}. Defaulting to {}",
          failedMetadataVolumesTolerated, FAILED_VOLUMES_TOLERATED_DEFAULT);
      failedMetadataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;
    }

    if (failedDbVolumesTolerated < -1) {
      LOG.warn(FAILED_DB_VOLUMES_TOLERATED_KEY +
              "must be greater than -1 and was set to {}. Defaulting to {}",
          failedDbVolumesTolerated, FAILED_VOLUMES_TOLERATED_DEFAULT);
      failedDbVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;
    }

    if (volumeIOTestCount == 0) {
      LOG.info("{} set to {}. Disk IO health tests have been disabled.",
          DISK_CHECK_IO_TEST_COUNT_KEY, volumeIOTestCount);
    } else {
      if (volumeIOTestCount < 0) {
        LOG.warn("{} must be greater than 0 but was set to {}." +
                "Defaulting to {}",
            DISK_CHECK_IO_TEST_COUNT_KEY, volumeIOTestCount,
            DISK_CHECK_IO_TEST_COUNT_DEFAULT);
        volumeIOTestCount = DISK_CHECK_IO_TEST_COUNT_DEFAULT;
      }

      if (volumeIOFailureTolerance < 0) {
        LOG.warn("{} must be greater than or equal to 0 but was set to {}. " +
                "Defaulting to {}",
            DISK_CHECK_IO_FAILURES_TOLERATED_KEY, volumeIOFailureTolerance,
            DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT);
        volumeIOFailureTolerance = DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT;
      }

      if (volumeIOFailureTolerance >= volumeIOTestCount) {
        LOG.warn("{} was set to {} but cannot be greater or equals to {} " +
                "set to {}. Defaulting {} to {} and {} to {}",
            DISK_CHECK_IO_FAILURES_TOLERATED_KEY, volumeIOFailureTolerance,
            DISK_CHECK_IO_TEST_COUNT_KEY, volumeIOTestCount,
            DISK_CHECK_IO_FAILURES_TOLERATED_KEY,
            DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT,
            DISK_CHECK_IO_TEST_COUNT_KEY, DISK_CHECK_IO_TEST_COUNT_DEFAULT);
        volumeIOTestCount = DISK_CHECK_IO_TEST_COUNT_DEFAULT;
        volumeIOFailureTolerance = DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT;
      }

      if (volumeHealthCheckFileSize < 1) {
        LOG.warn(DISK_CHECK_FILE_SIZE_KEY +
                "must be at least 1 byte and was set to {}. Defaulting to {}",
            volumeHealthCheckFileSize,
            DISK_CHECK_FILE_SIZE_DEFAULT);
        volumeHealthCheckFileSize =
            DISK_CHECK_FILE_SIZE_DEFAULT;
      }
    }

    if (diskCheckMinGap.isNegative()) {
      LOG.warn(DISK_CHECK_MIN_GAP_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          diskCheckMinGap, DISK_CHECK_MIN_GAP_DEFAULT);
      diskCheckMinGap = DISK_CHECK_MIN_GAP_DEFAULT;
    }

    if (diskCheckTimeout.isNegative()) {
      LOG.warn(DISK_CHECK_TIMEOUT_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          diskCheckTimeout, DISK_CHECK_TIMEOUT_DEFAULT);
      diskCheckTimeout = DISK_CHECK_TIMEOUT_DEFAULT;
    }

    if (blockDeleteCommandWorkerInterval.isNegative()) {
      LOG.warn(BLOCK_DELETE_COMMAND_WORKER_INTERVAL +
          " must be greater than zero and was set to {}. Defaulting to {}",
          blockDeleteCommandWorkerInterval,
          BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT);
      blockDeleteCommandWorkerInterval =
          BLOCK_DELETE_COMMAND_WORKER_INTERVAL_DEFAULT;
    }

    if (rocksdbLogMaxFileSize < 0) {
      LOG.warn(ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_KEY +
              " must be no less than zero and was set to {}. Defaulting to {}",
          rocksdbLogMaxFileSize, ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_DEFAULT);
      rocksdbLogMaxFileSize = ROCKSDB_LOG_MAX_FILE_SIZE_BYTES_DEFAULT;
    }

    if (rocksdbLogMaxFileNum <= 0) {
      LOG.warn(ROCKSDB_LOG_MAX_FILE_NUM_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          rocksdbLogMaxFileNum, ROCKSDB_LOG_MAX_FILE_NUM_DEFAULT);
      rocksdbLogMaxFileNum = ROCKSDB_LOG_MAX_FILE_NUM_DEFAULT;
    }

    if (rocksdbDeleteObsoleteFilesPeriod <= 0) {
      LOG.warn(ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          rocksdbDeleteObsoleteFilesPeriod,
          ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_DEFAULT);
      rocksdbDeleteObsoleteFilesPeriod =
          ROCKSDB_DELETE_OBSOLETE_FILES_PERIOD_MICRO_SECONDS_DEFAULT;
    }

    if (containerChecksumLockStripes < 1) {
      LOG.warn("{} must be at least 1. Defaulting to {}", CONTAINER_CHECKSUM_LOCK_STRIPES_KEY,
          CONTAINER_CHECKSUM_LOCK_STRIPES_DEFAULT);
      containerChecksumLockStripes = CONTAINER_CHECKSUM_LOCK_STRIPES_DEFAULT;
    }

    if (containerClientCacheSize < 1) {
      LOG.warn("{} must be at least 1. Defaulting to {}", CONTAINER_CLIENT_CACHE_SIZE,
          CONTAINER_CLIENT_CACHE_SIZE_DEFAULT);
      containerClientCacheSize = CONTAINER_CLIENT_CACHE_SIZE_DEFAULT;
    }

    if (containerClientCacheStaleThreshold < 1) {
      LOG.warn("{} must be at least 1. Defaulting to {}", CONTAINER_CLIENT_CACHE_STALE_THRESHOLD,
          CONTAINER_CLIENT_CACHE_STALE_THRESHOLD_MILLISECONDS_DEFAULT);
      containerClientCacheStaleThreshold =
          CONTAINER_CLIENT_CACHE_STALE_THRESHOLD_MILLISECONDS_DEFAULT;
    }

    validateMinFreeSpace();
  }

  /**
   * validate value of 'hdds.datanode.volume.min.free.space' and 'hdds.datanode.volume.min.free.space.percent'
   * and update with default value if not within range.
   */
  private void validateMinFreeSpace() {
    if (minFreeSpaceRatio > 1 || minFreeSpaceRatio < 0) {
      LOG.warn("{} = {} is invalid, should be between 0 and 1",
          HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT,
          minFreeSpaceRatio);
      minFreeSpaceRatio = HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_PERCENT_DEFAULT;
    }

    if (minFreeSpace < 0) {
      minFreeSpace = getDefaultFreeSpace();
    }
  }

  public Duration getBlockDeletionInterval() {
    return blockDeletionInterval;
  }

  public void setBlockDeletionInterval(Duration duration) {
    blockDeletionInterval = duration;
  }

  public Duration getRecoveringContainerScrubInterval() {
    return recoveringContainerScrubInterval;
  }

  public void setRecoveringContainerScrubInterval(Duration duration) {
    recoveringContainerScrubInterval = duration;
  }

  public int getBlockDeletionLimit() {
    return blockLimitPerInterval;
  }

  public void setBlockDeletionLimit(int limit) {
    this.blockLimitPerInterval = limit;
  }

  public long getDeleteContainerTimeoutMs() {
    return deleteContainerTimeoutMs;
  }

  public Duration getBlockDeletingMaxLockHoldingTime() {
    return Duration.ofMillis(blockDeletingMaxLockHoldingTime);
  }

  public void setBlockDeletingMaxLockHoldingTime(Duration maxLockHoldingTime) {
    blockDeletingMaxLockHoldingTime = maxLockHoldingTime.toMillis();
  }

  public boolean waitOnAllFollowers() {
    return waitOnAllFollowers;
  }

  public void setWaitOnAllFollowers(boolean val) {
    this.waitOnAllFollowers = val;
  }

  public int getContainerDeleteThreads() {
    return containerDeleteThreads;
  }

  public void setContainerDeleteThreads(int containerDeleteThreads) {
    this.containerDeleteThreads = containerDeleteThreads;
  }

  public int getContainerCloseThreads() {
    return containerCloseThreads;
  }

  public void setContainerCloseThreads(int containerCloseThreads) {
    this.containerCloseThreads = containerCloseThreads;
  }

  public long getMinFreeSpace(long capacity) {
    return Math.max((long) (capacity * minFreeSpaceRatio), minFreeSpace);
  }

  public long getMinFreeSpace() {
    return minFreeSpace;
  }

  public float getMinFreeSpaceRatio() {
    return minFreeSpaceRatio;
  }

  public long getPeriodicDiskCheckIntervalMinutes() {
    return periodicDiskCheckIntervalMinutes;
  }

  public void setPeriodicDiskCheckIntervalMinutes(long periodicDiskCheckIntervalMinutes) {
    this.periodicDiskCheckIntervalMinutes = periodicDiskCheckIntervalMinutes;
  }

  public int getFailedDataVolumesTolerated() {
    return failedDataVolumesTolerated;
  }

  public void setFailedDataVolumesTolerated(int failedVolumesTolerated) {
    this.failedDataVolumesTolerated = failedVolumesTolerated;
  }

  public int getFailedMetadataVolumesTolerated() {
    return failedMetadataVolumesTolerated;
  }

  public void setFailedMetadataVolumesTolerated(int failedVolumesTolerated) {
    this.failedMetadataVolumesTolerated = failedVolumesTolerated;
  }

  public int getFailedDbVolumesTolerated() {
    return failedDbVolumesTolerated;
  }

  public void setFailedDbVolumesTolerated(int failedVolumesTolerated) {
    this.failedDbVolumesTolerated = failedVolumesTolerated;
  }

  public int getVolumeIOTestCount() {
    return volumeIOTestCount;
  }

  public void setVolumeIOTestCount(int testCount) {
    this.volumeIOTestCount = testCount;
  }

  public int getVolumeIOFailureTolerance() {
    return volumeIOFailureTolerance;
  }

  public void setVolumeIOFailureTolerance(int failureTolerance) {
    volumeIOFailureTolerance = failureTolerance;
  }

  public int getVolumeHealthCheckFileSize() {
    return volumeHealthCheckFileSize;
  }

  public void getVolumeHealthCheckFileSize(int fileSizeBytes) {
    this.volumeHealthCheckFileSize = fileSizeBytes;
  }

  public boolean getCheckEmptyContainerDir() {
    return bCheckEmptyContainerDir;
  }

  public Duration getDiskCheckMinGap() {
    return diskCheckMinGap;
  }

  public void setDiskCheckMinGap(Duration duration) {
    diskCheckMinGap = duration;
  }

  public Duration getDiskCheckTimeout() {
    return diskCheckTimeout;
  }

  public void setDiskCheckTimeout(Duration duration) {
    diskCheckTimeout = duration;
  }

  public int getBlockDeleteThreads() {
    return blockDeleteThreads;
  }

  public void setBlockDeleteThreads(int threads) {
    this.blockDeleteThreads = threads;
  }

  public int getBlockDeleteQueueLimit() {
    return blockDeleteQueueLimit;
  }

  public void setBlockDeleteQueueLimit(int queueLimit) {
    this.blockDeleteQueueLimit = queueLimit;
  }

  public long getBlockDeleteMaxLockWaitTimeoutMs() {
    return blockDeleteMaxLockWaitTimeoutMs;
  }

  public Duration getBlockDeleteCommandWorkerInterval() {
    return blockDeleteCommandWorkerInterval;
  }

  public void setBlockDeleteCommandWorkerInterval(
      Duration blockDeleteCommandWorkerInterval) {
    this.blockDeleteCommandWorkerInterval = blockDeleteCommandWorkerInterval;
  }

  public int getCommandQueueLimit() {
    return cmdQueueLimit;
  }

  public void setCommandQueueLimit(int queueLimit) {
    this.cmdQueueLimit = queueLimit;
  }

  public boolean isChunkDataValidationCheck() {
    return isChunkDataValidationCheck;
  }

  public void setChunkDataValidationCheck(boolean writeChunkValidationCheck) {
    isChunkDataValidationCheck = writeChunkValidationCheck;
  }

  public int getNumReadThreadPerVolume() {
    return numReadThreadPerVolume;
  }

  public void setNumReadThreadPerVolume(int threads) {
    this.numReadThreadPerVolume = threads;
  }

  public boolean getContainerSchemaV3Enabled() {
    return this.containerSchemaV3Enabled;
  }

  public void setContainerSchemaV3Enabled(boolean containerSchemaV3Enabled) {
    this.containerSchemaV3Enabled = containerSchemaV3Enabled;
  }

  public String getContainerSchemaV3KeySeparator() {
    return this.containerSchemaV3KeySeparator;
  }

  public void setContainerSchemaV3KeySeparator(String separator) {
    this.containerSchemaV3KeySeparator = separator;
  }

  public String getRocksdbLogLevel() {
    return rocksdbLogLevel;
  }

  public void setRocksdbLogLevel(String level) {
    this.rocksdbLogLevel = level;
  }

  public void setRocksdbLogMaxFileNum(int count) {
    this.rocksdbLogMaxFileNum = count;
  }

  public int getRocksdbLogMaxFileNum() {
    return rocksdbLogMaxFileNum;
  }

  public void setRocksdbLogMaxFileSize(long size) {
    this.rocksdbLogMaxFileSize = size;
  }

  public long getRocksdbLogMaxFileSize() {
    return rocksdbLogMaxFileSize;
  }

  public long getRocksdbDeleteObsoleteFilesPeriod() {
    return rocksdbDeleteObsoleteFilesPeriod;
  }

  public void setRocksdbDeleteObsoleteFilesPeriod(long period) {
    this.rocksdbDeleteObsoleteFilesPeriod = period;
  }

  public void setRocksdbMaxOpenFiles(int count) {
    this.rocksdbMaxOpenFiles = count;
  }

  public int getRocksdbMaxOpenFiles() {
    return this.rocksdbMaxOpenFiles;
  }

  public boolean autoCompactionSmallSstFile() {
    return autoCompactionSmallSstFile;
  }

  public void setAutoCompactionSmallSstFile(boolean auto) {
    this.autoCompactionSmallSstFile = auto;
  }

  public long getAutoCompactionSmallSstFileSize() {
    return autoCompactionSmallSstFileSize;
  }

  public void setAutoCompactionSmallSstFileSize(long size) {
    this.autoCompactionSmallSstFileSize = size;
  }

  public int getAutoCompactionSmallSstFileNum() {
    return autoCompactionSmallSstFileNum;
  }

  public void setAutoCompactionSmallSstFileNum(int num) {
    this.autoCompactionSmallSstFileNum = num;
  }

  public int getContainerChecksumLockStripes() {
    return containerChecksumLockStripes;
  }

  public int getContainerClientCacheSize() {
    return containerClientCacheSize;
  }

  public int getContainerClientCacheStaleThreshold() {
    return containerClientCacheStaleThreshold;
  }

  public long getAutoCompactionSmallSstFileIntervalMinutes() {
    return autoCompactionSmallSstFileIntervalMinutes;
  }

  public void setAutoCompactionSmallSstFileIntervalMinutes(long autoCompactionSmallSstFileIntervalMinutes) {
    this.autoCompactionSmallSstFileIntervalMinutes =
        autoCompactionSmallSstFileIntervalMinutes;
  }

  public int getAutoCompactionSmallSstFileThreads() {
    return autoCompactionSmallSstFileThreads;
  }

  public void setAutoCompactionSmallSstFileThreads(int autoCompactionSmallSstFileThreads) {
    this.autoCompactionSmallSstFileThreads =
        autoCompactionSmallSstFileThreads;
  }

  static long getDefaultFreeSpace() {
    final StorageSize measure = StorageSize.parse(HDDS_DATANODE_VOLUME_MIN_FREE_SPACE_DEFAULT);
    return Math.round(measure.getUnit().toBytes(measure.getValue()));
  }
}
