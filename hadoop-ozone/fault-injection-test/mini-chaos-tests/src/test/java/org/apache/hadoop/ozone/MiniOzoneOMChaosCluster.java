/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.OzoneManager;

/**
 * This class causes random failures in OMs in the chaos cluster.
 */
public class MiniOzoneOMChaosCluster extends MiniOzoneChaosCluster {

  // Cluster is deemed ready for chaos when all the OMs are up and running.
  private AtomicBoolean isClusterReady = new AtomicBoolean(true);

  // The maximum number of nodes failures which can be tolerated without
  // losing quorum. This should be equal to (Num of OMs - 1)/2.
  private int numOfOMNodeFailuresTolerated;

  MiniOzoneOMChaosCluster(OzoneConfiguration conf,
      List<OzoneManager> ozoneManagers,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceID) {
    super(conf, ozoneManagers, scm, hddsDatanodes, omServiceID,
        FailureService.OZONE_MANAGER);
    setNumNodes(ozoneManagers.size());
    numOfOMNodeFailuresTolerated = (getNumNodes() - 1) / 2;
  }

  /**
   * Check if cluster is ready for a restart or shutdown of an OM node. If
   * yes, then set isClusterReady to false so that another thread cannot
   * restart/ shutdown OM till all OMs are up again.
   */
  protected boolean isClusterReady() {
    return isClusterReady.compareAndSet(true, false);
  }

  /**
   * If any OM node is not running, restart it.
   */
  @Override
  protected void getClusterReady()  {
    boolean clusterReady = true;
    for (OzoneManager om : getOzoneManagersList()) {
      if (!om.isRunning()) {
        try {
          restartOzoneManager(om, true);
        } catch (Exception e) {
          clusterReady = false;
          LOG.error("Cluster not ready for chaos. Failed to restart OM {}: {}",
              om.getOMNodeId(), e);
        }
      }
    }
    if (clusterReady) {
      isClusterReady.set(true);
    }
  }

  @Override
  protected int getNumberOfNodesToFail() {
    return RandomUtils.nextInt(1, numOfOMNodeFailuresTolerated + 1);
  }

  @Override
  protected void restartNode(int failedNodeIndex, boolean waitForNodeRestart)
      throws IOException, TimeoutException, InterruptedException {
    shutdownOzoneManager(failedNodeIndex);
    restartOzoneManager(failedNodeIndex, waitForNodeRestart);
    getClusterReady();
  }

  /**
   * For OM chaos, a shutdown node should eventually be restarted before the
   * next failure.
   */
  @Override
  protected void shutdownNode(int failedNodeIndex)
      throws ExecutionException, InterruptedException {
    shutdownOzoneManager(failedNodeIndex);

    // Restart the OM after FailureInterval / 2 duration.
    Executors.newSingleThreadScheduledExecutor().schedule(
        this::getClusterReady, getFailureIntervalInMS() / 2,
        TimeUnit.MILLISECONDS).get();
  }

  @Override
  protected String getFailedNodeID(int failedNodeIndex) {
    return getOzoneManager(failedNodeIndex).getOMNodeId();
  }

  /**
   * When restarting OM, always wait for it to catch up with Leader OM.
   */
  @Override
  protected boolean isFastRestart() {
    return true;
  }

  @Override
  protected boolean shouldStop() {
    return true;
  }
}
