package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import com.google.common.base.Preconditions;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

// TODO: Synchronization?
public class FinalizationStateManagerImpl implements FinalizationStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizationStateManagerImpl.class);

  private final Table<String, String> finalizationStore;
  private final DBTransactionBuffer transactionBuffer;
  // SCM transaction buffer flushes asynchronously, so we must keep the most
  // up-to-date DB information in memory as well for reads.
  private int dbMlv;
  private boolean hasFinalizingMark;
  private final List<ReplicatedFinalizationStep> finalizationSteps;
  private final HDDSLayoutVersionManager versionManager;

  private FinalizationStateManagerImpl(Builder builder) throws IOException {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.versionManager = builder.versionManager;

    if (finalizationStore.isExist(OzoneConsts.LAYOUT_VERSION_KEY)) {
      dbMlv = Integer
          .parseInt(finalizationStore.get(OzoneConsts.LAYOUT_VERSION_KEY));
    } else {
      // No layout version in the DB implies the initial layout version.
      dbMlv  = HDDSLayoutFeature.INITIAL_VERSION.layoutVersion();
    }
    hasFinalizingMark = finalizationStore.isExist(OzoneConsts.FINALIZING_KEY);
    this.finalizationSteps = new ArrayList<>();
  }

  public FinalizationCheckpoint getFinalizationCheckpoint() {
    boolean hasFinalizingMark = hasFinalizingMark();
    boolean mlvBehindSlv = versionManager.needsFinalization();

    FinalizationCheckpoint currentCheckpoint = null;
    // Enum constants must be iterated in order to resume from the correct
    // checkpoint.
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      if (checkpoint.isCurrent(hasFinalizingMark, mlvBehindSlv)) {
        currentCheckpoint = checkpoint;
        break;
      }
    }

    String errorMessage = String.format("SCM upgrade finalization " +
        "is in an unknown state.\nFinalizing mark present? %b\n" +
        "Metadata layout version behind software layout version? %b",
        hasFinalizingMark, mlvBehindSlv);
    Preconditions.checkNotNull(currentCheckpoint, errorMessage);
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

    // All prior checkpoints should have been crossed when this method is
    // called, leaving us at the finalization complete checkpoint.
    // If this is not the case, this SCM (leader or follower) has encountered
    // a bug leaving it in an inconsistent upgrade finalization state.
    // It should terminate to avoid further damage.
    FinalizationCheckpoint checkpoint = getFinalizationCheckpoint();
    if (checkpoint != FinalizationCheckpoint.FINALIZATION_COMPLETE) {
      String errorMessage = String.format("SCM upgrade finalization " +
              "is in an unknown state. Expected %s but was %s",
          FinalizationCheckpoint.FINALIZATION_COMPLETE, checkpoint);
      ExitUtils.terminate(1, errorMessage, LOG);
    }
  }

  @Override
  public boolean hasFinalizingMark() {
    return hasFinalizingMark;
  }

  @Override
  public void addReplicatedFinalizationStep(ReplicatedFinalizationStep step) {
    this.finalizationSteps.add(step);
  }

  public static final class Builder {
    private Table<String, String> finalizationStore;
    private DBTransactionBuffer transactionBuffer;
    private HDDSLayoutVersionManager versionManager;
    private SCMRatisServer scmRatisServer;

    public Builder() {
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public Builder setFinalizationStore(Table<String, String> finalizationStore) {
      this.finalizationStore = finalizationStore;
      return this;
    }

    public Builder setTransactionBuffer(DBTransactionBuffer transactionBuffer) {
      this.transactionBuffer = transactionBuffer;
      return this;
    }

    public Builder setVersionManager(HDDSLayoutVersionManager versionManager) {
      this.versionManager = versionManager;
      return this;
    }

    public FinalizationStateManager build() throws IOException {
      Preconditions.checkNotNull(finalizationStore);
      Preconditions.checkNotNull(transactionBuffer);
      Preconditions.checkNotNull(versionManager);

      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.FINALIZE,
              new FinalizationStateManagerImpl(this),
              scmRatisServer);

      return (FinalizationStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{FinalizationStateManager.class}, invocationHandler);
    }
  }
}
