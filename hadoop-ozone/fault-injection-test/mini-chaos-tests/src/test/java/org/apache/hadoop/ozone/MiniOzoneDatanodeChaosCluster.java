package org.apache.hadoop.ozone;

import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.OzoneManager;

public class MiniOzoneDatanodeChaosCluster extends MiniOzoneChaosCluster {

  public MiniOzoneDatanodeChaosCluster(OzoneConfiguration conf,
      List<OzoneManager> ozoneManagers,
      StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes,
      String omServiceID) {
    super(conf, ozoneManagers, scm, hddsDatanodes, omServiceID,
        FailureService.DATANODE);
    numNodes = hddsDatanodes.size();
  }

  @Override
  protected void restartNode(int failedNodeIndex, boolean waitForNodeRestart)
      throws TimeoutException, InterruptedException {
    restartHddsDatanode(failedNodeIndex, waitForNodeRestart);
  }

  @Override
  protected void shutdownNode(int failedNodeIndex) {
    shutdownHddsDatanode(failedNodeIndex);
  }

  @Override
  protected String getFailedNodeID(int failedNodeIndex) {
    return getHddsDatanodes().get(failedNodeIndex).getDatanodeDetails()
        .getUuidString();
  }
}
