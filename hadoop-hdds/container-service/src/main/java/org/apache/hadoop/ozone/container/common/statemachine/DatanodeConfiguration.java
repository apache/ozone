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

  static final int REPLICATION_MAX_STREAMS_DEFAULT = 10;

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

}
