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

/**
 * Interface for background services in SCM, including ReplicationManager,
 * SCMBlockDeletingService and BackgroundPipelineCreator.
 *
 * Provide a fine-grained method to manipulate the status of these background
 * services.
 */
public interface SCMService {
  /**
   * @param raftStatus latest raft status
   * @param safeModeStatus latest safe mode status
   */
  void notifyRaftStatusOrSafeModeStatusChanged(
      RaftStatus raftStatus, SafeModeStatus safeModeStatus);

  /**
   * @param raftStatus latest raft status
   * @param event latest triggered one time event.
   */
  default void notifyOneTimeEventTriggered(
      RaftStatus raftStatus, OneTimeEvent event) {
  }

  /**
   * @return true, if next iteration of Service should take effect,
   *         false, if next iteration of Service should be skipped.
   */
  boolean shouldRun();

  /**
   * @return name of the Service.
   */
  String getServiceName();

  /**
   * Status of Service.
   */
  enum ServiceStatus {
    RUNNING,
    PAUSING
  }

  /**
   * Raft related status.
   */
  enum RaftStatus {
    LEADER,
    NOT_LEADER
  }

  /**
   * Safe mode related status.
   */
  enum SafeModeStatus {
    IN_SAFE_MODE,
    OUT_OF_SAFE_MODE
  }

  /**
   * One time event.
   */
  enum OneTimeEvent {
    PRE_CHECK_COMPLETED,
    NEW_NODE_HANDLER_TRIGGERED,
    NON_HEALTHY_TO_HEALTHY_NODE_HANDLER_TRIGGERED
  }
}
