package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;
import java.util.Collections;

public class FinalizationManagerImpl implements FinalizationManager {
  private final SCMUpgradeFinalizer upgradeFinalizer;
  private final SCMUpgradeFinalizer.SCMUpgradeFinalizationContext context;
  private final SCMStorageConfig storage;


  public FinalizationManagerImpl(OzoneConfiguration conf,
                                 HDDSLayoutVersionManager versionManager,
                                 PipelineManager pipelineManager,
                                 NodeManager nodeManager,
                                 SCMStorageConfig storage,
                                 SCMHAManager scmHAManager,
                                 Table<String, String> finalizationStore) throws IOException {
    this.storage = storage;
    this.upgradeFinalizer = new SCMUpgradeFinalizer(versionManager);
    FinalizationStateManager finalizationStateManager =
        new FinalizationStateManagerImpl.Builder()
            .setVersionManager(versionManager)
            .setFinalizationStore(finalizationStore)
            .setTransactionBuffer(scmHAManager.getDBTransactionBuffer())
            .setRatisServer(scmHAManager.getRatisServer())
            .build();
    this.context =
        new SCMUpgradeFinalizer.SCMUpgradeFinalizationContext.Builder()
            .setStorage(this.storage)
            .setFinalizationStateManager(finalizationStateManager)
            .setConfiguration(conf)
            .setNodeManager(nodeManager)
            .setPipelineManager(pipelineManager)
            .setLayoutVersionManager(versionManager)
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
}
