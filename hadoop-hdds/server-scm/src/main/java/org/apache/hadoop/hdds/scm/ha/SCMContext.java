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

import com.google.common.base.Preconditions;
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
public class SCMContext implements EventHandler<SafeModeStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(SCMContext.class);

  private static final SCMContext EMPTY_CONTEXT
      = new SCMContext(true, 0, new SafeModeStatus(false, true), null);

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

  SCMContext(boolean isLeader, long term,
             final SafeModeStatus safeModeStatus,
             final StorageContainerManager scm) {
    this.isLeader = isLeader;
    this.term = term;
    this.safeModeStatus = safeModeStatus;
    this.scm = scm;
  }

  /**
   * Creates SCMContext instance from StorageContainerManager.
   */
  public SCMContext(final StorageContainerManager scm) {
    this(false, 0, new SafeModeStatus(true, false), scm);
    Preconditions.checkNotNull(scm, "scm is null");
  }

  /**
   *
   * @param newIsLeader : is leader or not
   * @param newTerm     : term if current SCM becomes leader
   */
  public void updateIsLeaderAndTerm(boolean newIsLeader, long newTerm) {
    lock.writeLock().lock();
    try {
      LOG.info("update <isLeader,term> from <{},{}> to <{},{}>",
          isLeader, term, newIsLeader, newTerm);

      isLeader = newIsLeader;
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
  public long getTerm() throws NotLeaderException {
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
  public void onMessage(SafeModeStatus status,
                        EventPublisher publisher) {
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
}
