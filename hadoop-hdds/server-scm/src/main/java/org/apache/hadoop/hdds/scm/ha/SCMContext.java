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
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
  private OptionalLong termOpt;
  private final StorageContainerManager scm;
  private final Lock lock = new ReentrantLock();

  private SCMContext(boolean isLeader, long term,
                     final StorageContainerManager scm) {
    this.isLeader = isLeader;
    this.termOpt = OptionalLong.of(term);
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
   * @param newIsLeader : is leader or not
   */
  public void updateIsLeader(boolean newIsLeader) {
    lock.lock();
    try {
      LOG.info("update isLeader from {} to {}", isLeader, newIsLeader);
      this.isLeader = newIsLeader;
      // update term lazily.
      this.termOpt = OptionalLong.empty();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Check whether current SCM is leader or not.
   *
   * @return isLeader
   */
  public boolean isLeader() {
    lock.lock();
    try {
      return isLeader;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get term of current leader SCM.
   *
   * @return term
   * @throws NotLeaderException if isLeader is false
   */
  public long getTerm() throws NotLeaderException {
    lock.lock();
    try {
      if (!isLeader) {
        LOG.warn("getTerm is invoked when not leader.");
        throw scm.getScmHAManager()
            .getRatisServer()
            .triggerNotLeaderException();
      }

      if (termOpt.isPresent()) {
        return termOpt.getAsLong();
      }

      // TODO: update term lazily.
      //       revisit the code when getCurrentTerm is exposed from Division.
      RaftServer.Division division =
          scm.getScmHAManager().getRatisServer().getDivision();

      RaftGroupId groupId = division.getGroup().getGroupId();
      RaftServerProxy server = (RaftServerProxy) division.getRaftServer();
      RaftServerImpl serverImpl = null;

      try {
        serverImpl = server.getImpl(groupId);
      } catch (IOException ioe) {
        String errorMessage = "Fail to get RaftServerImpl";
        ExitUtils.terminate(1, errorMessage, ioe, LOG);
      }

      // TODO: getRoleInfoProto() will be exposed from Division later.
      Preconditions.checkNotNull(serverImpl);
      RaftProtos.RoleInfoProto roleInfoProto = serverImpl.getRoleInfoProto();

      Preconditions.checkState(roleInfoProto.hasLeaderInfo());
      long term = roleInfoProto.getLeaderInfo().getTerm();
      LOG.info("Update term to {}.", term);

      termOpt = OptionalLong.of(term);
      return termOpt.getAsLong();
    } finally {
      lock.unlock();
    }
  }
}
