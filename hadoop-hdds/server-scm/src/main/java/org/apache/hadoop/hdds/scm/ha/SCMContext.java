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

import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SCMContext is the single source of truth for some key information shared
 * across all components within SCM, including:
 *  - RaftServer related info, e.g., isLeader, term.
 *  - SafeMode related info, e.g., inSafeMode, preCheckComplete.
 */
public final class SCMContext implements EventHandler<SafeModeStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(SCMContext.class);

  private static final SCMContext EMPTY_CONTEXT
      = new SCMContext.Builder().build();

  /**
   * Used by non-HA mode SCM, Recon and Unit Tests.
   */
  public static SCMContext emptyContext() {
    return EMPTY_CONTEXT;
  }

  /**
   * Raft related info.
   */
  private boolean isLeader;
  private long term;

  /**
   * Safe mode related info.
   */
  private SafeModeStatus safeModeStatus;

  private final StorageContainerManager scm;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private SCMContext(boolean isLeader, long term,
                     final SafeModeStatus safeModeStatus,
                     final StorageContainerManager scm) {
    this.isLeader = isLeader;
    this.term = term;
    this.safeModeStatus = safeModeStatus;
    this.scm = scm;
  }

  /**
   *
   * @param leader      : is leader or not
   * @param newTerm     : term if current SCM becomes leader
   */
  public void updateLeaderAndTerm(boolean leader, long newTerm) {
    lock.writeLock().lock();
    try {
      LOG.info("update <isLeader,term> from <{},{}> to <{},{}>",
          isLeader, term, leader, newTerm);

      isLeader = leader;
      term = newTerm;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Check whether current SCM is leader or not.
   *
   * @return isLeader
   */
  public boolean isLeader() {
    lock.readLock().lock();
    try {
      return isLeader;
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
      if (!isLeader) {
        LOG.warn("getTerm is invoked when not leader.");
        throw scm.getScmHAManager()
            .getRatisServer()
            .triggerNotLeaderException();
      }
      return term;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void onMessage(SafeModeStatus status, EventPublisher publisher) {
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

  public static class Builder {
    /**
     * As a leader of term 0, out of in safe mode, and has completed preCheck.
     */
    private boolean isLeader = true;
    private long term = 0;
    private boolean isInSafeMode = false;
    private boolean isPreCheckComplete = true;
    private StorageContainerManager scm = null;

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

    public Builder setSCM(StorageContainerManager storageContainerManager) {
      this.scm = storageContainerManager;
      return this;
    }

    public SCMContext build() {
      if (scm != null) {
        // SCMContext for HA mode.
        return new SCMContext(
            false,
            0,
            new SafeModeStatus(true, false),
            scm);
      } else {
        // SCMContext for Recon and Unit Tests.
        return new SCMContext(
            isLeader,
            term,
            new SafeModeStatus(isInSafeMode, isPreCheckComplete),
            null);
      }
    }
  }
}
