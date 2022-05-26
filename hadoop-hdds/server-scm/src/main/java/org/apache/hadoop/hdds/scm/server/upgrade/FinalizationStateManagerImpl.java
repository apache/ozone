package org.apache.hadoop.hdds.scm.server.upgrade;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FinalizationStateManagerImpl implements FinalizationStateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FinalizationStateManagerImpl.class);

  private final Table<String, String> finalizationStore;
  private final DBTransactionBuffer transactionBuffer;
  private final List<ReplicatedFinalizationStep> finalizationSteps;
  private final HDDSLayoutVersionManager versionManager;
  // Ensures that we are not in the process of updating checkpoint state as
  // we read it to determine the current checkpoint.
  private final ReadWriteLock checkpointLock;
  // SCM transaction buffer flushes asynchronously, so we must keep the most
  // up-to-date DB information in memory as well for reads.
  private volatile boolean hasFinalizingMark;

  private FinalizationStateManagerImpl(Builder builder) throws IOException {
    this.finalizationStore = builder.finalizationStore;
    this.transactionBuffer = builder.transactionBuffer;
    this.versionManager = builder.versionManager;
    this.checkpointLock = new ReentrantReadWriteLock();

    hasFinalizingMark = finalizationStore.isExist(OzoneConsts.FINALIZING_KEY);
    this.finalizationSteps = new ArrayList<>();
  }

  @Override
  public void addFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = true;
    } finally {
      checkpointLock.writeLock().unlock();
    }
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.FINALIZING_KEY, "");
  }

  @Override
  public void finalizeLayoutFeature(Integer layoutVersion) throws IOException {
    checkpointLock.writeLock().lock();
    try {
      LayoutFeature feature = versionManager.getFeature(layoutVersion);
      for(ReplicatedFinalizationStep step: finalizationSteps) {
        step.run(feature);
      }
    } finally {
      checkpointLock.writeLock().unlock();
    }
    transactionBuffer.addToBuffer(finalizationStore,
        OzoneConsts.LAYOUT_VERSION_KEY, String.valueOf(layoutVersion));
  }

  @Override
  public void removeFinalizingMark() throws IOException {
    checkpointLock.writeLock().lock();
    try {
      hasFinalizingMark = false;
    } finally {
      checkpointLock.writeLock().unlock();
    }
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
  public void addReplicatedFinalizationStep(ReplicatedFinalizationStep step) {
    this.finalizationSteps.add(step);
  }

  @Override
  public boolean passedCheckpoint(FinalizationCheckpoint checkpoint) {
    return getFinalizationCheckpoint().compareTo(checkpoint) >= 0;
  }

  private FinalizationCheckpoint getFinalizationCheckpoint() {
    // Get a point-in-time snapshot of the finalization state under the lock,
    // then use this to determine which checkpoint we were on at that time.
    boolean mlvBehindSlvSnapshot;
    boolean hasFinalizingMarkSnapshot;
    checkpointLock.readLock().lock();
    try {
      mlvBehindSlvSnapshot = versionManager.needsFinalization();
      hasFinalizingMarkSnapshot = hasFinalizingMark;
    } finally {
      checkpointLock.readLock().unlock();
    }

    FinalizationCheckpoint currentCheckpoint = null;
    for (FinalizationCheckpoint checkpoint: FinalizationCheckpoint.values()) {
      if (checkpoint.isCurrent(hasFinalizingMarkSnapshot,
          mlvBehindSlvSnapshot)) {
        currentCheckpoint = checkpoint;
        break;
      }
    }

    String errorMessage = String.format("SCM upgrade finalization " +
            "is in an unknown state.\nFinalizing mark present? %b\n" +
            "Metadata layout version behind software layout version? %b",
        hasFinalizingMarkSnapshot, mlvBehindSlvSnapshot);
    Preconditions.checkNotNull(currentCheckpoint, errorMessage);
    return currentCheckpoint;
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

    public FinalizationStateManager buildForTesting() throws IOException {
      return getInstance();
    }

    public FinalizationStateManager build() throws IOException {
      final SCMHAInvocationHandler invocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.FINALIZE,
              getInstance(),
              scmRatisServer);

      return (FinalizationStateManager) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{FinalizationStateManager.class}, invocationHandler);
    }

    private FinalizationStateManager getInstance() throws IOException {
      Preconditions.checkNotNull(finalizationStore);
      Preconditions.checkNotNull(transactionBuffer);
      Preconditions.checkNotNull(versionManager);
      return new FinalizationStateManagerImpl(this);
    }
  }
}
