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
    numNodes = ozoneManagers.size();
    numOfOMNodeFailuresTolerated = (numNodes - 1) / 2;
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
    for (OzoneManager om : ozoneManagers) {
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
        this::getClusterReady, failureIntervalInMS / 2,
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
