/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SCMContext is the single source of truth for some key information shared
 * across all components within SCM, including:
 * 1) RaftServer related info, e.g., isLeader, term.
 * 2) SafeMode related info, e.g., inSafeMode, preCheckComplete.
 *
 * If current SCM is not running upon Ratis, the {@link SCMContext#isLeader}
 * check will always return true, and {@link SCMContext#getTermOfLeader} will
 * return INVALID_TERM.
 */
public final class SCMContext {
  private static final Logger LOG = LoggerFactory.getLogger(SCMContext.class);

  /**
   * The initial value of term in raft is 0, and term increases monotonically.
   * term equals INVALID_TERM indicates current SCM is running without Ratis.
   */
  public static final long INVALID_TERM = -1;

  /**
   * Used by non-HA mode SCM, Recon and Unit Tests.
   */
  public static SCMContext emptyContext() {
    return new SCMContext.Builder().buildMaybeInvalid();
  }

  /**
   * Raft related info.
   */
  private boolean isLeader;
  private boolean isLeaderReady;
  private long term;

  /**
   * Safe mode related info.
   */
  private SafeModeStatus safeModeStatus;

  private final OzoneStorageContainerManager scm;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Tracks the last crossed SCM upgrade finalization checkpoint.
   */
  private volatile FinalizationCheckpoint finalizationCheckpoint;

  private SCMContext(boolean isLeader, long term,
      final SafeModeStatus safeModeStatus,
      final FinalizationCheckpoint finalizationCheckpoint,
      final OzoneStorageContainerManager scm) {
    this.isLeader = isLeader;
    this.term = term;
    this.safeModeStatus = safeModeStatus;
    this.finalizationCheckpoint = finalizationCheckpoint;
    this.scm = scm;
    this.isLeaderReady = false;
  }

  /**
   * @param leader  : is leader or not
   * @param newTerm : term if current SCM becomes leader
   */
  public void updateLeaderAndTerm(boolean leader, long newTerm) {
    lock.writeLock().lock();
    try {
      LOG.info("update <isLeader,term> from <{},{}> to <{},{}>",
          isLeader, term, leader, newTerm);

      isLeader = leader;
      // If it is not leader, set isLeaderReady to false.
      if (!isLeader) {
        isLeaderReady = false;
        LOG.info("update <isLeaderReady> from <{}> to <{}>", isLeaderReady,
            false);
      }
      term = newTerm;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Set isLeaderReady flag to true, this indicate leader is ready to accept
   * transactions.
   *
   * On the leader SCM once all the previous leader term transaction are
   * applied, this will be called to set the isLeaderReady to true.
   *
   */
  public void setLeaderReady() {
    lock.writeLock().lock();
    try {
      LOG.info("update <isLeaderReady> from <{}> to <{}>",
          isLeaderReady, true);

      isLeaderReady = true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setFinalizationCheckpoint(FinalizationCheckpoint checkpoint) {
    lock.writeLock().lock();
    try {
      this.finalizationCheckpoint = checkpoint;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Check whether current SCM is leader or not.
   *
   * Use this API to know if SCM can send a command to DN once after it is
   * elected as leader.
   * True - it is leader, else false.
   *
   * @return isLeader
   */
  public boolean isLeader() {
    lock.readLock().lock();
    try {
      if (term == INVALID_TERM) {
        return true;
      }

      return isLeader;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check whether current SCM is leader ready.
   *
   * Use this API to know when all the previous leader term transactions are
   * applied and the SCM DB/in-memory state is latest state and then only
   * particular command/action need to be taken by SCM.
   *
   * In general all background services should use this API to start their
   * service.
   *
   * True - it is leader and ready, else false.
   *
   * @return isLeaderReady
   */
  public boolean isLeaderReady() {
    lock.readLock().lock();
    try {
      if (term == INVALID_TERM) {
        return true;
      }

      return isLeaderReady;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get term of current leader SCM.
   *
   * @return term
   * @throws NotLeaderException if isLeader is false
   */
  public long getTermOfLeader() throws NotLeaderException {
    lock.readLock().lock();
    try {
      if (term == INVALID_TERM) {
        return term;
      }

      if (!isLeader) {
        LOG.warn("getTerm is invoked when not leader.");
        if (scm instanceof StorageContainerManager) {
          StorageContainerManager storageContainerManager =
                  (StorageContainerManager) scm;
          throw storageContainerManager.getScmHAManager()
                  .getRatisServer()
                  .triggerNotLeaderException();
        }
      }
      return term;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @param status : update SCMContext with latest SafeModeStatus.
   */
  public void updateSafeModeStatus(SafeModeStatus status) {
    lock.writeLock().lock();
    try {
      LOG.info("Update SafeModeStatus from {} to {}.", safeModeStatus, status);
      safeModeStatus = status;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public boolean isInSafeMode() {
    lock.readLock().lock();
    try {
      return safeModeStatus.isInSafeMode();
    } finally {
      lock.readLock().unlock();
    }
  }

  public boolean isPreCheckComplete() {
    lock.readLock().lock();
    try {
      return safeModeStatus.isPreCheckComplete();
    } finally {
      lock.readLock().unlock();
    }
  }

  public FinalizationCheckpoint getFinalizationCheckpoint() {
    lock.readLock().lock();
    try {
      return this.finalizationCheckpoint;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @return StorageContainerManager
   */
  public OzoneStorageContainerManager getScm() {
    return scm;
  }

  /**
   * Builder for SCMContext.
   */
  public static class Builder {
    /**
     * The default context:
     * running without Ratis, out of safe mode, and has completed preCheck.
     */
    private boolean isLeader = false;
    private long term = INVALID_TERM;
    private boolean isInSafeMode = false;
    private boolean isPreCheckComplete = true;
    private OzoneStorageContainerManager scm = null;
    private FinalizationCheckpoint finalizationCheckpoint;

    public Builder setLeader(boolean leader) {
      this.isLeader = leader;
      return this;
    }

    public Builder setTerm(long newTerm) {
      this.term = newTerm;
      return this;
    }

    public Builder setIsInSafeMode(boolean inSafeMode) {
      this.isInSafeMode = inSafeMode;
      return this;
    }

    public Builder setIsPreCheckComplete(boolean preCheckComplete) {
      this.isPreCheckComplete = preCheckComplete;
      return this;
    }

    public Builder setSCM(
        OzoneStorageContainerManager storageContainerManager) {
      this.scm = storageContainerManager;
      return this;
    }

    public Builder setFinalizationCheckpoint(
        FinalizationCheckpoint checkpoint) {
      this.finalizationCheckpoint = checkpoint;
      return this;
    }

    public SCMContext build() {
      Preconditions.checkNotNull(scm, "scm == null");
      return buildMaybeInvalid();
    }

    /**
     * Allows {@code null} SCM.  Only for tests.
     */
    @VisibleForTesting
    SCMContext buildMaybeInvalid() {
      return new SCMContext(
          isLeader,
          term,
          new SafeModeStatus(isInSafeMode, isPreCheckComplete),
          Optional.ofNullable(finalizationCheckpoint).orElse(
              FinalizationCheckpoint.FINALIZATION_COMPLETE),
          scm);
    }
  }
}
