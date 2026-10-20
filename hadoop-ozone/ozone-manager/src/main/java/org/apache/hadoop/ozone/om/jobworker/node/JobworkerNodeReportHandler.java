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

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.NodeReportFromJobworker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Node Reports from Jobworker.
 */
public class JobworkerNodeReportHandler implements EventHandler<NodeReportFromJobworker> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(JobworkerNodeReportHandler.class);
  private final JobworkerNodeManager nodeManager;

  public JobworkerNodeReportHandler(JobworkerNodeManager nodeManager) {
    Objects.requireNonNull(nodeManager);
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(NodeReportFromJobworker nodeReportFromJobworker,
                        EventPublisher publisher) {
    Objects.requireNonNull(nodeReportFromJobworker);
    JobworkerDetails jobworker = nodeReportFromJobworker.getJobworkerDetails();
    Objects.requireNonNull(jobworker, "NodeReport is "
        + "missing JobworkerDetails.");
    nodeManager.processNodeReport(jobworker, nodeReportFromJobworker.getReport());
  }
}
