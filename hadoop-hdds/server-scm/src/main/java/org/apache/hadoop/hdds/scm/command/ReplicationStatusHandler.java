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

package org.apache.hadoop.hdds.scm.command;

import org.apache.hadoop.hdds.scm.command.CommandStatusReportHandler.ReplicationStatus;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles REPLICATION_STATUS events by clearing failed pending ops.
 * Only the leader SCM processes these events; followers skip them.
 */
public class ReplicationStatusHandler implements EventHandler<ReplicationStatus> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationStatusHandler.class);

  private final ContainerReplicaPendingOps containerReplicaPendingOps;
  private final SCMContext scmContext;

  public ReplicationStatusHandler(ContainerReplicaPendingOps containerReplicaPendingOps,
      SCMContext scmContext) {
    this.containerReplicaPendingOps = containerReplicaPendingOps;
    this.scmContext = scmContext;
  }

  @Override
  public void onMessage(ReplicationStatus status, EventPublisher publisher) {
    if (!scmContext.isLeader()) {
      LOG.info("Skip processing replication status since current SCM is not leader.");
      return;
    }
    status.getCmdStatus().forEach(cmdStatus ->
        containerReplicaPendingOps.onReplicationCommandFailed(cmdStatus.getCmdId()));
  }

}
