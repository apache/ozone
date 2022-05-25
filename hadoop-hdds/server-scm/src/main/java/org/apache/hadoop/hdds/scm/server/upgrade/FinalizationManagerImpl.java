package org.apache.hadoop.hdds.scm.server.upgrade;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.DefaultUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizer.SCMUpgradeFinalizationContext;
import org.rocksdb.Checkpoint;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class FinalizationManagerImpl implements FinalizationManager {
  private final SCMUpgradeFinalizer upgradeFinalizer;
  private final SCMUpgradeFinalizer.SCMUpgradeFinalizationContext context;
  private final SCMStorageConfig storage;
  private final FinalizationStateManager finalizationStateManager;

  private FinalizationManagerImpl(Builder builder) throws IOException {
    this.storage = builder.storage;
    this.upgradeFinalizer = new SCMUpgradeFinalizer(builder.versionManager,
        builder.executor);
    finalizationStateManager =
        new FinalizationStateManagerImpl.Builder()
            .setVersionManager(builder.versionManager)
            .setFinalizationStore(builder.finalizationStore)
            .setTransactionBuffer(builder.scmHAManager.getDBTransactionBuffer())
            .setRatisServer(builder.scmHAManager.getRatisServer())
            .build();
    this.context =
        new SCMUpgradeFinalizer.SCMUpgradeFinalizationContext.Builder()
            .setStorage(this.storage)
            .setFinalizationStateManager(finalizationStateManager)
            .setConfiguration(builder.conf)
            .setNodeManager(builder.nodeManager)
            .setPipelineManager(builder.pipelineManager)
            .setLayoutVersionManager(builder.versionManager)
            .build();
    finalizationStateManager.addReplicatedFinalizationStep(
        lf -> this.upgradeFinalizer.replicatedFinalizationSteps(lf, context));
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException {
    return upgradeFinalizer.finalize(upgradeClientID, context);
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException {
    if (readonly) {
      return new UpgradeFinalizer.StatusAndMessages(upgradeFinalizer.getStatus(),
          Collections.emptyList());
    }
    return upgradeFinalizer.reportStatus(upgradeClientID, takeover);
  }

  @Override
  public BasicUpgradeFinalizer<SCMUpgradeFinalizer.SCMUpgradeFinalizationContext, HDDSLayoutVersionManager> getUpgradeFinalizer() {
    return upgradeFinalizer;
  }

  @Override
  public void runPrefinalizeStateActions() throws IOException  {
    upgradeFinalizer.runPrefinalizeStateActions(storage, context);
  }

  @Override
  public boolean passedCheckpoint(FinalizationStateManager.FinalizationCheckpoint checkpoint) {
    return finalizationStateManager.passedCheckpoint(checkpoint);
  }

  public static final class Builder {
    private OzoneConfiguration conf;
    private HDDSLayoutVersionManager versionManager;
    private PipelineManager pipelineManager;
    private NodeManager nodeManager;
    private SCMStorageConfig storage;
    private SCMHAManager scmHAManager;
    private Table<String, String> finalizationStore;
    private UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor;

    public Builder() {
    }

    public Builder setConfiguration(OzoneConfiguration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setLayoutVersionManager(HDDSLayoutVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public Builder setPipelineManager(PipelineManager pipelineManager) {
      this.pipelineManager = pipelineManager;
      return this;
    }

    public Builder setNodeManager(NodeManager nodeManager) {
      this.nodeManager = nodeManager;
      return this;
    }

    public Builder setStorage(SCMStorageConfig storage) {
      this.storage = storage;
      return this;
    }

    public Builder setHAManager(SCMHAManager scmHAManager) {
      this.scmHAManager = scmHAManager;
      return this;
    }

    public Builder setFinalizationStore(Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public Builder setFinalizationExecutor(
        UpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> executor) {
      this.executor = executor;
      return this;
    }

    public FinalizationManagerImpl build() throws IOException {
      Preconditions.checkNotNull(conf);
      Preconditions.checkNotNull(versionManager);
      Preconditions.checkNotNull(pipelineManager);
      Preconditions.checkNotNull(nodeManager);
      Preconditions.checkNotNull(storage);
      Preconditions.checkNotNull(scmHAManager);
      Preconditions.checkNotNull(finalizationStore);
      executor = Optional.ofNullable(executor)
          .orElse(new DefaultUpgradeFinalizationExecutor<>());
      return new FinalizationManagerImpl(this);
    }
  }
}
