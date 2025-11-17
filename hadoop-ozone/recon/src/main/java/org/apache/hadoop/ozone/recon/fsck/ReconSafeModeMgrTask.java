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

package org.apache.hadoop.ozone.recon.fsck;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL_DEFAULT;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconSafeModeManager;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that scans the list of containers and keeps track if
 * recon warm up completed, and it exits safe mode.
 */
public class ReconSafeModeMgrTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconSafeModeMgrTask.class);

  private ContainerManager containerManager;
  private ReconNodeManager nodeManager;
  private ReconSafeModeManager safeModeManager;
  private List<DatanodeInfo> allNodes;
  private List<ContainerInfo> containers;
  private final long interval;
  private final long dnHBInterval;

  public ReconSafeModeMgrTask(
      ContainerManager containerManager,
      ReconNodeManager nodeManager,
      ReconSafeModeManager safeModeManager,
      ReconTaskConfig reconTaskConfig,
      OzoneConfiguration ozoneConfiguration) {
    this.safeModeManager = safeModeManager;
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
    this.allNodes = nodeManager.getAllNodes();
    this.containers = containerManager.getContainers();
    interval = reconTaskConfig.getSafeModeWaitThreshold().toMillis();
    dnHBInterval = ozoneConfiguration.getTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        HDDS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  public synchronized void start() {
    long timeElapsed = 0L;
    try {
      tryReconExitSafeMode();
      while (safeModeManager.getInSafeMode() && timeElapsed <= interval) {
        wait(dnHBInterval);
        timeElapsed += dnHBInterval;
        allNodes = nodeManager.getAllNodes();
        containers = containerManager.getContainers();
        tryReconExitSafeMode();
      }
      // Exceeded safe mode grace period. Exit safe mode
      if (safeModeManager.getInSafeMode()) {
        LOG.warn("Recon could not exit safe mode after {} ms. Exiting safe mode anyway. " +
            "Please check for any unexpected startup issues", timeElapsed);
        safeModeManager.setInSafeMode(false);
      } else {
        LOG.info("Recon exited safe mode after {} ms.", timeElapsed);
      }
    } catch (Throwable t) {
      LOG.error("Exception in ReconSafeModeMgrTask Thread.", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void tryReconExitSafeMode() {
      // Recon starting first time
    if (null == allNodes || allNodes.isEmpty()) {
      return;
    }
    if (null == containers || containers.isEmpty()) {
      return;
    }
    final Set<ContainerID> currentContainersInAllDatanodes =
        new HashSet<>(containers.size());
    allNodes.forEach(node -> {
      try {
        currentContainersInAllDatanodes.addAll(
            nodeManager.getContainers(node));
      } catch (NodeNotFoundException e) {
        LOG.error("Node not found: {}", node);
      }
    });
    if (containers.size() == currentContainersInAllDatanodes.size()) {
      safeModeManager.setInSafeMode(false);
    }
  }
}
