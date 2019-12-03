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
import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class used for high level datanode configuration parameters.
 */
@ConfigGroup(prefix = "hdds.datanode")
public class DatanodeConfiguration {
  static final Logger LOG =
      LoggerFactory.getLogger(DatanodeConfiguration.class);

  /**
   * The maximum number of replication commands a single datanode can execute
   * simultaneously.
   */
  private final int replicationMaxStreamsDefault = 10;
  private int replicationMaxStreams = replicationMaxStreamsDefault;
  /**
   * The maximum number of threads used to delete containers on a datanode
   * simultaneously.
   */
  private final int containerDeleteThreadsDefault = 2;
  private int containerDeleteThreads = containerDeleteThreadsDefault;

  @Config(key = "replication.streams.limit",
      type = ConfigType.INT,
      defaultValue = "10",
      tags = {DATANODE},
      description = "The maximum number of replication commands a single " +
          "datanode can execute simultaneously"
  )
  public void setReplicationMaxStreams(int val) {
    if (val < 1) {
      LOG.warn("hdds.datanode.replication.streams.limit must be greater than" +
          "zero and was set to {}. Defaulting to {}",
          val, replicationMaxStreamsDefault);
      replicationMaxStreams = replicationMaxStreamsDefault;
    } else {
      this.replicationMaxStreams = val;
    }
  }

  public int getReplicationMaxStreams() {
    return replicationMaxStreams;
  }

  @Config(key = "container.delete.threads.max",
      type = ConfigType.INT,
      defaultValue = "2",
      tags = {DATANODE},
      description = "The maximum number of threads used to delete containers " +
          "on a datanode"
  )
  public void setContainerDeleteThreads(int val) {
    if (val < 1) {
      LOG.warn("hdds.datanode.container.delete.threads.max must be greater " +
          "than zero and was set to {}. Defaulting to {}",
          val, containerDeleteThreadsDefault);
      containerDeleteThreads = containerDeleteThreadsDefault;
    } else {
      this.containerDeleteThreads = val;
    }
  }

  public int getContainerDeleteThreads() {
    return containerDeleteThreads;
  }

}
