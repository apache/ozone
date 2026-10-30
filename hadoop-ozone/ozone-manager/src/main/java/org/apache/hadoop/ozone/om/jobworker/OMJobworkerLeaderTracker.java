/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.jobworker;

import java.util.OptionalLong;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ha.OMService;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the Raft term and leadership of this OM for the Jobworker framework.
 * <p>
 * Commands are stamped with the term of the ready leader. When this OM stops being the ready leader, or the term
 * changes, all in-flight and queued commands are dropped, since a new leader rebuilds its dispatch state from scratch.
 */
public class OMJobworkerLeaderTracker implements OMService {

  private static final Logger LOG = LoggerFactory.getLogger(OMJobworkerLeaderTracker.class);

  private final OzoneManager om;
  private volatile OMJobworkerCommandManager commandManager;
  private volatile JobworkerNodeManager nodeManager;
  // Term in which this OM was last seen as the ready leader, or empty.
  private OptionalLong readyTerm = OptionalLong.empty();

  public OMJobworkerLeaderTracker(OzoneManager om) {
    this.om = om;
  }

  /**
   * Starts dropping the state of the given managers on leadership changes.
   */
  public synchronized void attach(OMJobworkerCommandManager cmdManager, JobworkerNodeManager nodeMgr) {
    this.commandManager = cmdManager;
    this.nodeManager = nodeMgr;
    this.readyTerm = getTermIfReady();
  }

  /**
   * Stops tracking, for example when the Jobworker service is stopped.
   */
  public synchronized void detach() {
    this.commandManager = null;
    this.nodeManager = null;
    this.readyTerm = OptionalLong.empty();
  }

  /**
   * @return the current term if this OM is the leader and ready to serve, otherwise empty.
   */
  public OptionalLong getTermIfReady() {
    DivisionInfo info = getDivisionInfo();
    if (info == null || !info.isLeaderReady()) {
      return OptionalLong.empty();
    }
    long term = info.getCurrentTerm();
    // The term may have moved on while it was read.
    return info.isLeaderReady() && info.getCurrentTerm() == term ? OptionalLong.of(term) : OptionalLong.empty();
  }

  /**
   * @return the current term if this OM is the leader, even if it is not yet ready, otherwise empty.
   */
  public OptionalLong getTermIfLeader() {
    DivisionInfo info = getDivisionInfo();
    return info != null && info.isLeader() ? OptionalLong.of(info.getCurrentTerm()) : OptionalLong.empty();
  }

  public boolean isLeaderReady() {
    return getTermIfReady().isPresent();
  }

  @Override
  public synchronized void notifyStatusChanged() {
    OMJobworkerCommandManager cmdManager = commandManager;
    JobworkerNodeManager nodeMgr = nodeManager;
    OptionalLong current = getTermIfReady();
    if (cmdManager == null || nodeMgr == null || current.equals(readyTerm)) {
      readyTerm = current;
      return;
    }
    // Becoming the ready leader for the first time needs no reset, but losing it or a new term does.
    if (readyTerm.isPresent()) {
      LOG.info("Jobworker leader term changed from {} to {}, dropping in-flight and queued commands",
          readyTerm, current);
      nodeMgr.clearAllCommands();
      cmdManager.onLeadershipChange();
    }
    readyTerm = current;
  }

  @Override
  public boolean shouldRun() {
    return true;
  }

  @Override
  public String getServiceName() {
    return OMJobworkerLeaderTracker.class.getSimpleName();
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  private DivisionInfo getDivisionInfo() {
    OzoneManagerRatisServer ratisServer = om.getOmRatisServer();
    if (ratisServer == null) {
      return null;
    }
    RaftServer.Division division = ratisServer.getServerDivision();
    return division == null ? null : division.getInfo();
  }
}
