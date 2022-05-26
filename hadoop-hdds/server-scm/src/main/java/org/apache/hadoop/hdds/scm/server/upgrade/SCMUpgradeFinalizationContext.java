package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;

/**
 * Provided to methods in the {@link SCMUpgradeFinalizer} to supply objects
 * needed to operate.
 */
public final class SCMUpgradeFinalizationContext {
  private final PipelineManager pipelineManager;
  private final NodeManager nodeManager;
  private final FinalizationStateManager finalizationStateManager;
  private final SCMStorageConfig storage;
  private final HDDSLayoutVersionManager versionManager;
  private final OzoneConfiguration conf;

  private SCMUpgradeFinalizationContext(Builder builder) {
    pipelineManager = builder.pipelineManager;
    nodeManager = builder.nodeManager;
    finalizationStateManager = builder.finalizationStateManager;
    storage = builder.storage;
    versionManager = builder.versionManager;
    conf = builder.conf;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public PipelineManager getPipelineManager() {
    return pipelineManager;
  }

  public FinalizationStateManager getFinalizationStateManager() {
    return finalizationStateManager;
  }

  public OzoneConfiguration getConfiguration() {
    return conf;
  }

  public HDDSLayoutVersionManager getLayoutVersionManager() {
    return versionManager;
  }

  public SCMStorageConfig getStorage() {
    return storage;
  }

  /**
   * Builds an {@link SCMUpgradeFinalizationContext}.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private PipelineManager pipelineManager;
    private NodeManager nodeManager;
    private FinalizationStateManager finalizationStateManager;
    private SCMStorageConfig storage;
    private HDDSLayoutVersionManager versionManager;
    private OzoneConfiguration conf;

    public Builder() {
    }

    public Builder setPipelineManager(PipelineManager pipelineManager) {
      this.pipelineManager = pipelineManager;
      return this;
    }

    public Builder setNodeManager(NodeManager nodeManager) {
      this.nodeManager = nodeManager;
      return this;
    }

    public Builder setFinalizationStateManager(
        FinalizationStateManager finalizationStateManager) {
      this.finalizationStateManager = finalizationStateManager;
      return this;
    }

    public Builder setStorage(SCMStorageConfig storage) {
      this.storage = storage;
      return this;
    }

    public Builder setLayoutVersionManager(
        HDDSLayoutVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public Builder setConfiguration(OzoneConfiguration conf) {
      this.conf = conf;
      return this;
    }

    public SCMUpgradeFinalizationContext build() {
      return new SCMUpgradeFinalizationContext(this);
    }
  }
}
