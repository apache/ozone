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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.Queues;
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
    underRepQueue = Queues.synchronizedQueue(new PriorityQueue<>(
        Comparator.comparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getWeightedRedundancy)
        .thenComparing(ContainerHealthResult
            .UnderReplicatedHealthResult::getRequeueCount)));
    overRepQueue = Queues.synchronizedQueue(new LinkedList<>());
  }

  /**
   * Add an under replicated container back to the queue if it was unable to
   * be processed. Its retry count will be incremented before it is re-queued,
   * reducing its priority.
   * Note that the queue could have been rebuilt and replaced after this
   * message was removed but before it is added back. This will result in a
   * duplicate entry on the queue. However, when it is processed again, the
   * result of the processing will end up with pending replicas scheduled. If
   * instance 1 is processed and creates the pending replicas, when instance 2
   * is processed, it will find the pending containers and know it has no work
   * to do, and be discarded. Additionally, the queue will be refreshed
   * periodically removing any duplicates.
   */
  public void enqueue(ContainerHealthResult.UnderReplicatedHealthResult
      underReplicatedHealthResult) {
    underReplicatedHealthResult.incrementRequeueCount();
    underRepQueue.add(underReplicatedHealthResult);
  }

  public void enqueue(ContainerHealthResult.OverReplicatedHealthResult
      overReplicatedHealthResult) {
    overRepQueue.add(overReplicatedHealthResult);
  }

  /**
   * Retrieve the new highest priority container to be replicated from the
   * under-replicated queue.
   * @return The new underReplicated container to be processed, or null if the
   *         queue is empty.
   */
  public ContainerHealthResult.UnderReplicatedHealthResult
      dequeueUnderReplicatedContainer() {
    return underRepQueue.poll();
  }

  /**
   * Retrieve the new highest priority container to be replicated from the
   * over-replicated queue.
   * @return The next over-replicated container to be processed, or null if the
   *         queue is empty.
   */
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

  public boolean isEmpty() {
    return underRepQueue.isEmpty() && overRepQueue.isEmpty();
  }

}
