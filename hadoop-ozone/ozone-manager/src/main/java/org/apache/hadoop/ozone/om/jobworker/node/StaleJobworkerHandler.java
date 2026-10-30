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

package org.apache.hadoop.ozone.om.jobworker.node;

import java.util.List;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.om.jobworker.command.OMJobworkerCommandManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Stale JobWorker event.
 * When a JobWorker becomes stale, we mark it as unavailable for task assignments
 * and reschedule its pending tasks to other healthy nodes.
 */
public class StaleJobworkerHandler implements EventHandler<JobworkerDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(StaleJobworkerHandler.class);

  private final JobworkerNodeManager nodeManager;
  private final OMJobworkerCommandManager omJobworkerCommandManager;

  public StaleJobworkerHandler(JobworkerNodeManager nodeManager,
                               OMJobworkerCommandManager omJobworkerCommandManager) {
    this.nodeManager = nodeManager;
    this.omJobworkerCommandManager = omJobworkerCommandManager;
  }

  @Override
  public void onMessage(JobworkerDetails jobworkerDetails,
                        EventPublisher publisher) {
    LOG.info("JobWorker {} moved to stale state. Marking as unavailable for task assignment.",
        jobworkerDetails);

    // For any pending commands targeted at this JobWorker, we can clear them
    // since the JobWorker is no longer reachable
    List<OMJobworkerCommand> commands =
        nodeManager.pollJobworkerCommand(jobworkerDetails.getUuid());
    omJobworkerCommandManager.markCommandsFailedForJobworker(commands, jobworkerDetails);

    LOG.info("JobWorker {} marked as stale: updated {} commands to FAILED state",
        jobworkerDetails.getUuidString(), commands.size());
  }
}
