/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.scm.ha.SCMService.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Manipulate background services in SCM, including ReplicationManager,
 * SCMBlockDeletingService and BackgroundPipelineCreator.
 */
public class SCMServiceManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMServiceManager.class);

  // Cached latest raft status and safe mode status.
  private RaftStatus raftStatus;
  private SafeModeStatus safeModeStatus;

  private final List<SCMService> services = new ArrayList<>();

  /**
   * Start as a follower SCM in safe mode.
   */
  public SCMServiceManager() {
    raftStatus = RaftStatus.NOT_LEADER;
    safeModeStatus = SafeModeStatus.IN_SAFE_MODE;
  }

  /**
   * Register a SCMService to SCMServiceManager.
   */
  public synchronized void register(SCMService service) {
    Preconditions.checkNotNull(service);
    LOG.info("register Service {}", service.getServiceName());
    services.add(service);
  }

  /**
   * Current SCM becomes leader.
   */
  public synchronized void becomeLeader() {
    LOG.info("SCM becomes leader.");
    raftStatus = RaftStatus.LEADER;
    notifyRaftStatusOrSafeModeStatusChanged();
  }

  /**
   * Current SCM steps down.
   */
  public synchronized void stepDown() {
    LOG.info("SCM steps down.");
    raftStatus = RaftStatus.NOT_LEADER;
    notifyRaftStatusOrSafeModeStatusChanged();
  }

  /**
   * Current SCM enters into safe mode,
   * e.g., restart or reload SCMStateMachine.
   */
  public synchronized void enteringSafeMode() {
    LOG.info("SCM enters SafeMode.");
    safeModeStatus = SafeModeStatus.IN_SAFE_MODE;
    notifyRaftStatusOrSafeModeStatusChanged();
  }

  /**
   * Current SCM leaves safe mode.
   */
  public synchronized void leavingSafeMode() {
    LOG.info("SCM leaves SafeMode.");
    safeModeStatus = SafeModeStatus.OUT_OF_SAFE_MODE;
    notifyRaftStatusOrSafeModeStatusChanged();
  }

  /**
   * Called when one-time event happens.
   */
  public synchronized void triggeringOneTimeEvent(OneTimeEvent event) {
    LOG.info("OneTimeEvent is triggered with {}.", event);
    for (SCMService service : services) {
      LOG.info("Notify service:{} with raftStatus:{} oneTimeEvent:{}",
          service.getServiceName(), raftStatus, event);
      service.notifyOneTimeEventTriggered(raftStatus, event);
    }
  }

  // iterate services, update them with the latest status.
  private void notifyRaftStatusOrSafeModeStatusChanged() {
    for (SCMService service : services) {
      LOG.info("Notify service:{} with raftStatus:{} safeModeStatus:{}",
          service.getServiceName(), raftStatus, safeModeStatus);
      service.notifyRaftStatusOrSafeModeStatusChanged(
          raftStatus, safeModeStatus);
    }
  }
}
