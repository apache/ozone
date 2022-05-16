package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;
import java.util.Collections;

public class FinalizationManagerImpl implements FinalizationManager {
  // TODO: Do we need this as a field?
  private HDDSLayoutVersionManager versionManager;
  private SCMUpgradeFinalizer upgradeFinalizer;
  private FinalizationStateManager finalizationStateManager;


  public FinalizationManagerImpl(HDDSLayoutVersionManager versionManager,
                                 PipelineManager pipelineManager,
                                 NodeManager nodeManager,
                                 DBTransactionBuffer transactionBuffer,
                                 Table<String, String> finalizationStore) throws IOException {
    this.versionManager = versionManager;
    this.upgradeFinalizer = new SCMUpgradeFinalizer(versionManager);
    this.finalizationStateManager =
        new FinalizationStateManagerImpl(finalizationStore, transactionBuffer
            , lf -> upgradeFinalizer.finalizeLayoutFeature(lf, ));
  }

  @Override
  public void markFinalizationStarted() throws IOException {
    finalizationStateManager.addFinalizingMark();
  }

  @Override
  public void finalizeLayoutFeature(HDDSLayoutFeature layoutFeature) throws IOException {
    finalizationStateManager.setDBMetadataLayoutVersion((long)layoutFeature.layoutVersion());
  }

  @Override
  public void markFinalizationCompleted() throws IOException {
    finalizationStateManager.removeFinalizingMark();
  }

  @Override
  public UpgradeFinalizer.StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException {
    return upgradeFinalizer.finalize(upgradeClientID, this);
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
}
