package org.apache.hadoop.ozone.recon.scm;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Recon's version of SCM Node Manager.
 * TODO This is just an initial implementation. Will be revisited in future.
 */
public class ReconNodeManager extends SCMNodeManager {

  public ReconNodeManager(OzoneConfiguration conf,
                          SCMStorageConfig scmStorageConfig,
                          EventPublisher eventPublisher,
                          NetworkTopology networkTopology) {
    super(conf, scmStorageConfig, eventPublisher, networkTopology);
  }
}
