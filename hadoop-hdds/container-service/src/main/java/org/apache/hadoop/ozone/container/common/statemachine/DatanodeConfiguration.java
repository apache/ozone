/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.statemachine;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.ConfigTag;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Configuration class used for high level datanode configuration parameters.
 */
@ConfigGroup(prefix = "hdds.datanode")
public class DatanodeConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeConfiguration.class);

  static final String REPLICATION_STREAMS_LIMIT_KEY =
      "hdds.datanode.replication.streams.limit";
  static final String CONTAINER_DELETE_THREADS_MAX_KEY =
      "hdds.datanode.container.delete.threads.max";
  static final String PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY =
      "hdds.datanode.periodic.disk.check.interval.minutes";
  public static final String FAILED_DATA_VOLUMES_TOLERATED_KEY =
      "hdds.datanode.failed.data.volumes.tolerated";
  public static final String FAILED_METADATA_VOLUMES_TOLERATED_KEY =
      "hdds.datanode.failed.metadata.volumes.tolerated";
  public static final String DISK_CHECK_MIN_GAP_KEY =
      "hdds.datanode.disk.check.min.gap";
  public static final String DISK_CHECK_TIMEOUT_KEY =
      "hdds.datanode.disk.check.timeout";

  static final int REPLICATION_MAX_STREAMS_DEFAULT = 10;

  static final long PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT = 60;

  static final int FAILED_VOLUMES_TOLERATED_DEFAULT = -1;

  static final long DISK_CHECK_MIN_GAP_DEFAULT =
      Duration.ofMinutes(15).toMillis();

  static final long DISK_CHECK_TIMEOUT_DEFAULT =
      Duration.ofMinutes(10).toMillis();

  /**
   * The maximum number of replication commands a single datanode can execute
   * simultaneously.
   */
  @Config(key = "replication.streams.limit",
      type = ConfigType.INT,
      defaultValue = "10",
      tags = {DATANODE},
      description = "The maximum number of replication commands a single " +
          "datanode can execute simultaneously"
  )
  private int replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;

  static final int CONTAINER_DELETE_THREADS_DEFAULT = 2;
  static final int BLOCK_DELETE_THREADS_DEFAULT = 5;

  /**
   * The maximum number of threads used to delete containers on a datanode
   * simultaneously.
   */
  @Config(key = "container.delete.threads.max",
      type = ConfigType.INT,
      defaultValue = "2",
      tags = {DATANODE},
      description = "The maximum number of threads used to delete containers " +
          "on a datanode"
  )
  private int containerDeleteThreads = CONTAINER_DELETE_THREADS_DEFAULT;

  /**
   * The maximum number of threads used to handle delete block commands.
   * It takes about 200ms to open a RocksDB with HDD media, so basically DN
   * can handle 300 individual container delete tx every 60s if RocksDB cache
   * missed. With max threads 5, optimistically DN can handle 1500 individual
   * container delete tx in 60s with RocksDB cache miss.
   */
  @Config(key = "block.delete.threads.max",
      type = ConfigType.INT,
      defaultValue = "5",
      tags = {DATANODE},
      description = "The maximum number of threads used to handle delete " +
          " blocks on a datanode"
  )
  private int blockDeleteThreads = BLOCK_DELETE_THREADS_DEFAULT;

  /**
   * The maximum number of commands in queued list.
   * 1440 = 60 * 24, which means if SCM send a delete command every minute,
   * if the commands are pined up for more than 1 day, DN will start to discard
   * new comming commands.
   */
  @Config(key = "block.delete.queue.limit",
      type = ConfigType.INT,
      defaultValue = "1440",
      tags = {DATANODE},
      description = "The maximum number of block delete commands queued on "+
          " a datanode"
  )
  private int blockDeleteQueueLimit = 60 * 24;

  @Config(key = "block.deleting.service.interval",
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
  private long blockDeletionInterval = Duration.ofSeconds(60).toMillis();

  public Duration getBlockDeletionInterval() {
    return Duration.ofMillis(blockDeletionInterval);
  }

  public void setBlockDeletionInterval(Duration duration) {
    this.blockDeletionInterval = duration.toMillis();
  }

  @Config(key = "block.deleting.limit.per.interval",
      defaultValue = "5000",
      type = ConfigType.INT,
      tags = { ConfigTag.SCM, ConfigTag.DELETION },
      description =
          "Number of blocks to be deleted in an interval."
  )
  private int blockLimitPerInterval = 5000;

  public int getBlockDeletionLimit() {
    return blockLimitPerInterval;
  }

  public void setBlockDeletionLimit(int limit) {
    this.blockLimitPerInterval = limit;
  }

  @Config(key = "periodic.disk.check.interval.minutes",
      defaultValue = "60",
      type = ConfigType.LONG,
      tags = { DATANODE },
      description = "Periodic disk check run interval in minutes."
  )
  private long periodicDiskCheckIntervalMinutes =
      PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;

  @Config(key = "failed.data.volumes.tolerated",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of data volumes that are allowed to fail "
          + "before a datanode stops offering service. "
          + "Config this to -1 means unlimited, but we should have "
          + "at least one good volume left."
  )
  private int failedDataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;

  @Config(key = "failed.metadata.volumes.tolerated",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = { DATANODE },
      description = "The number of metadata volumes that are allowed to fail "
          + "before a datanode stops offering service. "
          + "Config this to -1 means unlimited, but we should have "
          + "at least one good volume left."
  )
  private int failedMetadataVolumesTolerated = FAILED_VOLUMES_TOLERATED_DEFAULT;

  @Config(key = "disk.check.min.gap",
      defaultValue = "15m",
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "The minimum gap between two successive checks of the same"
          + " Datanode volume. Unit could be defined with"
          + " postfix (ns,ms,s,m,h,d)."
  )
  private long diskCheckMinGap = DISK_CHECK_MIN_GAP_DEFAULT;

  @Config(key = "disk.check.timeout",
      defaultValue = "10m",
      type = ConfigType.TIME,
      tags = { DATANODE },
      description = "Maximum allowed time for a disk check to complete."
          + " If the check does not complete within this time interval"
          + " then the disk is declared as failed. Unit could be defined with"
          + " postfix (ns,ms,s,m,h,d)."
  )
  private long diskCheckTimeout = DISK_CHECK_TIMEOUT_DEFAULT;

  @PostConstruct
  public void validate() {
    if (replicationMaxStreams < 1) {
      LOG.warn(REPLICATION_STREAMS_LIMIT_KEY + " must be greater than zero " +
              "and was set to {}. Defaulting to {}",
          replicationMaxStreams, REPLICATION_MAX_STREAMS_DEFAULT);
      replicationMaxStreams = REPLICATION_MAX_STREAMS_DEFAULT;
    }

    if (containerDeleteThreads < 1) {
      LOG.warn(CONTAINER_DELETE_THREADS_MAX_KEY + " must be greater than zero" +
              " and was set to {}. Defaulting to {}",
          containerDeleteThreads, CONTAINER_DELETE_THREADS_DEFAULT);
      containerDeleteThreads = CONTAINER_DELETE_THREADS_DEFAULT;
    }

    if (periodicDiskCheckIntervalMinutes < 1) {
      LOG.warn(PERIODIC_DISK_CHECK_INTERVAL_MINUTES_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          periodicDiskCheckIntervalMinutes,
          PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT);
      periodicDiskCheckIntervalMinutes =
          PERIODIC_DISK_CHECK_INTERVAL_MINUTES_DEFAULT;
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

    if (diskCheckMinGap < 0) {
      LOG.warn(DISK_CHECK_MIN_GAP_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          diskCheckMinGap, DISK_CHECK_MIN_GAP_DEFAULT);
      diskCheckMinGap = DISK_CHECK_MIN_GAP_DEFAULT;
    }

    if (diskCheckTimeout < 0) {
      LOG.warn(DISK_CHECK_TIMEOUT_KEY +
              " must be greater than zero and was set to {}. Defaulting to {}",
          diskCheckTimeout, DISK_CHECK_TIMEOUT_DEFAULT);
      diskCheckTimeout = DISK_CHECK_TIMEOUT_DEFAULT;
    }
  }

  public void setReplicationMaxStreams(int replicationMaxStreams) {
    this.replicationMaxStreams = replicationMaxStreams;
  }

  public void setContainerDeleteThreads(int containerDeleteThreads) {
    this.containerDeleteThreads = containerDeleteThreads;
  }

  public int getReplicationMaxStreams() {
    return replicationMaxStreams;
  }

  public int getContainerDeleteThreads() {
    return containerDeleteThreads;
  }

  public long getPeriodicDiskCheckIntervalMinutes() {
    return periodicDiskCheckIntervalMinutes;
  }

  public void setPeriodicDiskCheckIntervalMinutes(
      long periodicDiskCheckIntervalMinutes) {
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

  public Duration getDiskCheckMinGap() {
    return Duration.ofMillis(diskCheckMinGap);
  }

  public void setDiskCheckMinGap(Duration duration) {
    this.diskCheckMinGap = duration.toMillis();
  }

  public Duration getDiskCheckTimeout() {
    return Duration.ofMillis(diskCheckTimeout);
  }

  public void setDiskCheckTimeout(Duration duration) {
    this.diskCheckTimeout = duration.toMillis();
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
}
