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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Node;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background pipeline sync task that queries pipelines in SCM, and removes
 * any obsolete pipeline. Also syncs operational state of dead nodes with SCM
 * state.
 */
public class PipelineSyncTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineSyncTask.class);

  private StorageContainerServiceProvider scmClient;
  private ReconPipelineManager reconPipelineManager;
  private ReconNodeManager nodeManager;

  private ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final long interval;
  private final ReconTaskStatusUpdater taskStatusUpdater;

  public PipelineSyncTask(ReconPipelineManager pipelineManager,
      ReconNodeManager nodeManager,
      StorageContainerServiceProvider scmClient,
      ReconTaskConfig reconTaskConfig,
      ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    super(taskStatusUpdaterManager);
    this.scmClient = scmClient;
    this.reconPipelineManager = pipelineManager;
    this.nodeManager = nodeManager;
    this.interval = reconTaskConfig.getPipelineSyncTaskInterval().toMillis();
    this.taskStatusUpdater = getTaskStatusUpdater();
  }

  @Override
  public void run() {
    try {
      while (canRun()) {
        initializeAndRunTask();
        Thread.sleep(interval);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Pipeline sync Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      taskStatusUpdater.setLastTaskRunStatus(-1);
      taskStatusUpdater.recordRunCompletion();
    }
  }

  @Override
  protected void runTask() throws IOException, NodeNotFoundException {
    lock.writeLock().lock();
    try {
      long start = Time.monotonicNow();
      List<Pipeline> pipelinesFromScm = scmClient.getPipelines();
      reconPipelineManager.initializePipelines(pipelinesFromScm);
      syncOperationalStateOnDeadNodes();
      LOG.debug("Pipeline sync Thread took {} milliseconds.",
          Time.monotonicNow() - start);
      taskStatusUpdater.setLastTaskRunStatus(0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * For every dead node in Recon, update Operational state with that on SCM
   * if different.
   * @throws IOException on Error
   * @throws NodeNotFoundException if node not found in Recon.
   */
  private void syncOperationalStateOnDeadNodes()
      throws IOException, NodeNotFoundException {
    List<DatanodeDetails> deadNodesOnRecon = nodeManager.getNodes(null, DEAD);

    if (!deadNodesOnRecon.isEmpty()) {
      List<Node> scmNodes = scmClient.getNodes();
      List<Node> filteredScmNodes = scmNodes.stream()
              .filter(n -> deadNodesOnRecon.contains(
                  DatanodeDetails.getFromProtoBuf(n.getNodeID())))
              .collect(Collectors.toList());

      for (Node deadNode : filteredScmNodes) {
        DatanodeDetails dnDetails =
            DatanodeDetails.getFromProtoBuf(deadNode.getNodeID());

        HddsProtos.NodeState scmNodeState = deadNode.getNodeStates(0);
        if (scmNodeState != DEAD) {
          LOG.warn("Node {} DEAD in Recon, but SCM reports it as {}",
              dnDetails.getHostName(), scmNodeState);
        }
        nodeManager.updateNodeOperationalStateFromScm(deadNode, dnDetails);
      }
    }
  }
}
