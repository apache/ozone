/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Object to encapsulate the under and over replication queues used by
 * replicationManager.
 */
public class ReplicationQueue {

  private final Queue<ContainerHealthResult.UnderReplicatedHealthResult>
      underRepQueue;
  private final Queue<ContainerHealthResult.OverReplicatedHealthResult>
      overRepQueue;

  public ReplicationQueue() {
    underRepQueue = new PriorityQueue<>(
        Comparator.comparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getWeightedRedundancy)
        .thenComparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getRequeueCount));
    overRepQueue = new LinkedList<>();
  }

  public void enqueue(ContainerHealthResult.UnderReplicatedHealthResult
      underReplicatedHealthResult) {
    underRepQueue.add(underReplicatedHealthResult);
  }

  public void enqueue(ContainerHealthResult.OverReplicatedHealthResult
      overReplicatedHealthResult) {
    overRepQueue.add(overReplicatedHealthResult);
  }

  public ContainerHealthResult.UnderReplicatedHealthResult
      dequeueUnderReplicatedContainer() {
    return underRepQueue.poll();
  }

  public ContainerHealthResult.OverReplicatedHealthResult
      dequeueOverReplicatedContainer() {
    return overRepQueue.poll();
  }

  public int underReplicatedQueueSize() {
    return underRepQueue.size();
  }

  public int overReplicatedQueueSize() {
    return overRepQueue.size();
  }

}
