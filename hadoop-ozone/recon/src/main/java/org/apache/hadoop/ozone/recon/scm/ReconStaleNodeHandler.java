package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.StaleNodeHandler;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's handling of Stale node.
 */
public class ReconStaleNodeHandler extends StaleNodeHandler {

  private static final Logger LOG =
        LoggerFactory.getLogger(ReconDeadNodeHandler.class);
  private PipelineSyncTask pipelineSyncTask;

  public ReconStaleNodeHandler(NodeManager nodeManager,
                                 PipelineManager pipelineManager,
                                 OzoneConfiguration conf,
                                 PipelineSyncTask pipelineSyncTask) {
    super(nodeManager, pipelineManager, conf);
    this.pipelineSyncTask = pipelineSyncTask;
  }

  @Override
  public void onMessage(final DatanodeDetails datanodeDetails,
                          final EventPublisher publisher) {
    super.onMessage(datanodeDetails, publisher);
    try {
      pipelineSyncTask.triggerPipelineSyncTask();
    } catch (Exception exp) {
      LOG.error("Error trying to trigger pipeline sync task..",
          exp);
    }
  }
}
