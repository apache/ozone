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

import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.CommandStatusReportFromJobworker;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.NodeReportFromJobworker;

/**
 * Class that defines events related to Jobworker nodes.
 */
public final class OMJobworkerEvents {

  /**
   * Process the node report sent by Jobworker.
   */
  public static final TypedEvent<NodeReportFromJobworker>
      JW_NODE_REPORT = new TypedEvent<>(NodeReportFromJobworker.class,
      "JW_Node_Report");

  public static final TypedEvent<CommandStatusReportFromJobworker>
      JW_COMMAND_STATUS_REPORT = new TypedEvent<>(CommandStatusReportFromJobworker.class,
      "JW_Command_Status_Report");

  /**
   * Event for when a Jobworker node becomes stale.
   */
  public static final TypedEvent<JobworkerDetails>
      STALE_JOBWORKER = new TypedEvent<>(JobworkerDetails.class, "Stale_Jobworker");

  /**
   * Event for when a new Jobworker node is registered.
   */
  public static final TypedEvent<JobworkerDetails>
      NEW_JOBWORKER = new TypedEvent<>(JobworkerDetails.class, "New_Jobworker");

  /**
   * Private constructor. This class should not be instantiated.
   */
  private OMJobworkerEvents() {
  }
}
