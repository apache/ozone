/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class used to pick messages from the ReplicationManager over replicated
 * queue, calculate the delete commands and assign to the datanodes
 * via the eventQueue.
 */
public class OverReplicatedProcessor implements Runnable {

  private static final Logger LOG = LoggerFactory
      .getLogger(OverReplicatedProcessor.class);
  private final ReplicationManager replicationManager;
  private volatile boolean runImmediately = false;
  private final long intervalInMillis;

  public OverReplicatedProcessor(ReplicationManager replicationManager,
      long intervalInMillis) {
    this.replicationManager = replicationManager;
    this.intervalInMillis = intervalInMillis;
  }

  /**
   * Read messages from the ReplicationManager over replicated queue and,
   * form commands to correct the over replication. The commands are added
   * to the event queue and the PendingReplicaOps are adjusted.
   *
   * Note: this is a temporary implementation of this feature. A future
   * version will need to limit the amount of messages assigned to each
   * datanode, so they are not assigned too much work.
   */
  public void processAll() {
    int processed = 0;
    int failed = 0;
    while (true) {
      if (!replicationManager.shouldRun()) {
        break;
      }
      ContainerHealthResult.OverReplicatedHealthResult overRep =
          replicationManager.dequeueOverReplicatedContainer();
      if (overRep == null) {
        break;
      }
      try {
        processContainer(overRep);
        processed++;
      } catch (Exception e) {
        LOG.error("Error processing over replicated container {}",
            overRep.getContainerInfo(), e);
        failed++;
        replicationManager.requeueOverReplicatedContainer(overRep);
      }
    }
    LOG.info("Processed {} over replicated containers, failed processing {}",
        processed, failed);
  }

  protected void processContainer(ContainerHealthResult
      .OverReplicatedHealthResult overRep) throws IOException {
    Map<DatanodeDetails, SCMCommand<?>> cmds = replicationManager
        .processOverReplicatedContainer(overRep);
    for (Map.Entry<DatanodeDetails, SCMCommand<?>> cmd : cmds.entrySet()) {
      SCMCommand<?> scmCmd = cmd.getValue();
      replicationManager.sendDatanodeCommand(scmCmd, overRep.getContainerInfo(),
          cmd.getKey());
    }
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        if (replicationManager.shouldRun()) {
          processAll();
        }
        synchronized (this) {
          if (!runImmediately) {
            wait(intervalInMillis);
          }
          runImmediately = false;
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("{} interrupted. Exiting...", Thread.currentThread().getName());
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  synchronized void runImmediately() {
    runImmediately = true;
    notify();
  }
}
