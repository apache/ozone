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
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
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
public class SCMContext {
  private static final Logger LOG = LoggerFactory.getLogger(SCMContext.class);

  private static final SCMContext EMPTY_CONTEXT
      = new SCMContext(true, 0, null);

  /**
   * Used by non-HA mode SCM, Recon and Unit Tests.
   */
  public static SCMContext emptyContext() {
    return EMPTY_CONTEXT;
  }

  private boolean isLeader;
  private long term;
  private final StorageContainerManager scm;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private SCMContext(boolean isLeader, long term,
                     final StorageContainerManager scm) {
    this.isLeader = isLeader;
    this.term = term;
    this.scm = scm;
  }

  /**
   * Creates SCMContext instance from StorageContainerManager.
   */
  public SCMContext(final StorageContainerManager scm) {
    this(false, 0, scm);
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

      this.isLeader = newIsLeader;
      this.term = newTerm;
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
}
