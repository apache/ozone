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

import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Class used to pick messages from the ReplicationManager under replicated
 * queue, calculate the reconstruction commands and assign to the datanodes
 * via the eventQueue.
 */
public class UnderReplicatedProcessor extends UnhealthyReplicationProcessor
        <ContainerHealthResult.UnderReplicatedHealthResult> {

  UnderReplicatedProcessor(ReplicationManager replicationManager,
      Supplier<Duration> interval) {
    super(replicationManager, interval);
  }

  @Override
  protected ContainerHealthResult.UnderReplicatedHealthResult
      dequeueHealthResultFromQueue(ReplicationQueue queue) {
    return queue.dequeueUnderReplicatedContainer();
  }

  @Override
  protected void requeueHealthResult(ReplicationQueue queue,
      ContainerHealthResult.UnderReplicatedHealthResult healthResult) {
    queue.enqueue(healthResult);
  }

  @Override
  protected boolean inflightOperationLimitReached(ReplicationManager rm,
      long pendingOpLimit) {
    return rm.getInflightReplicationCount() >= pendingOpLimit;
  }

  @Override
  protected int sendDatanodeCommands(
          ReplicationManager replicationManager,
          ContainerHealthResult.UnderReplicatedHealthResult healthResult)
          throws IOException {
    return replicationManager.processUnderReplicatedContainer(healthResult);
  }
}
