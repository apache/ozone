package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: Synchronization?
public class FinalizationStateManagerImpl implements FinalizationStateManager {
  private final Table<String, String> finalizationStore;
  private final DBTransactionBuffer transactionBuffer;
  // SCM transaction buffer flushes asynchronously, so we must keep the most
  // up-to-date DB information in memory as well for reads.
  private long dbMlv;
  private boolean hasFinalizingMark;
  private final List<ReplicatedFinalizationStep> finalizationSteps;
  private final HDDSLayoutVersionManager versionManager;

  public FinalizationStateManagerImpl(HDDSLayoutVersionManager versionManager,
      Table<String, String> finalizationStore,
      DBTransactionBuffer transactionBuffer) throws IOException {
    this.finalizationStore = finalizationStore;
    this.transactionBuffer = transactionBuffer;
    this.versionManager = versionManager;

    dbMlv =
        Long.parseLong(finalizationStore.get(OzoneConsts.LAYOUT_VERSION_KEY));
    hasFinalizingMark = finalizationStore.isExist(OzoneConsts.FINALIZING_KEY);
    this.finalizationSteps = new ArrayList<>();

    // TODO: Return proxy here for @Replicate?
  }

  public FinalizationCheckpoint getFinalizationCheckpoint() {
    boolean hasFinalizingMark = hasFinalizingMark();
    boolean mlvBehindSlv = versionManager.needsFinalization();

    FinalizationCheckpoint currentCheckpoint = null;
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      if (checkpoint.isPassed(hasFinalizingMark, mlvBehindSlv)) {
        currentCheckpoint = checkpoint;
      } else {
        break;
      }
    }

    String errorMessage = String.format("SCM upgrade finalization " +
        "is in an unknown state.\nFinalizing mark present? %b\n" +
        "Metadata layout version behind software layout version? %b",
        hasFinalizingMark, mlvBehindSlv);
    Preconditions.assertNotNull(currentCheckpoint, errorMessage);
    return currentCheckpoint;
  }


  @Override
  public void addFinalizingMark() throws IOException {
    hasFinalizingMark = true;
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY, "");
  }

  @Override
  public void finalizeLayoutFeature(Integer layoutVersion) throws IOException {
    LayoutFeature feature = versionManager.getFeature(layoutVersion);
    for(ReplicatedFinalizationStep step: finalizationSteps) {
      step.run(feature);
    }
    dbMlv = layoutVersion;
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutVersion));
  }

  @Override
  public void removeFinalizingMark() throws IOException {
    hasFinalizingMark = false;
    transactionBuffer.removeFromBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY);
  }

  @Override
  public long getDBMetadataLayoutVersion() {
    return dbMlv;
  }

  @Override
  public boolean hasFinalizingMark() {
    return hasFinalizingMark;
  }

  @Override
  public void addReplicatedFinalizationStep(ReplicatedFinalizationStep step) {
    this.finalizationSteps.add(step);
  }
}
