/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * StorageClass interface.
 */
public interface StorageClass {

  OpenStateConfiguration getOpenStateConfiguration();

  // TODO(baoloongmao): Use this to implement replication factor two
  ClosedStateConfiguration getClosedStateConfiguration();

  String getName();

  /**
   * The open state configuration.
   */
  class OpenStateConfiguration {

    private final HddsProtos.ReplicationType replicationType;

    private final HddsProtos.ReplicationFactor replicationFactor;

    public OpenStateConfiguration(
        HddsProtos.ReplicationType replicationType,
        HddsProtos.ReplicationFactor replicationFactor
    ) {
      this.replicationType = replicationType;
      this.replicationFactor = replicationFactor;
    }

    public HddsProtos.ReplicationType getReplicationType() {
      return replicationType;
    }

    public HddsProtos.ReplicationFactor getReplicationFactor() {
      return replicationFactor;
    }
  }

  /**
   * The close state configuration.
   */
  class ClosedStateConfiguration {

    private final int replicationFactor;

    public ClosedStateConfiguration(int replicationFactor) {
      this.replicationFactor = replicationFactor;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }
  }
}
