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

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

/**
 * Class defining Safe mode exit criteria according to number of DataNodes
 * registered with SCM.
 */
public class DataNodeSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport> {

  private static final String NAME = "DataNodeSafeModeRule";

  // Min DataNodes required to exit safe mode.
  private int requiredDns;
  private NodeManager nodeManager;

  public DataNodeSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      NodeManager nodeManager,
      SCMSafeModeManager manager) {
    super(manager, NAME, eventQueue);
    requiredDns = conf.getInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE_DEFAULT);
    this.nodeManager = nodeManager;
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return null;
  }

  @Override
  protected boolean validate() {
    return nodeManager.getNodes(NodeStatus.inServiceHealthy()).size() >= requiredDns;
  }

  @Override
  protected void cleanup() {
    // No longer needed as registeredDnSet is removed.
  }

  @Override
  public String getStatusText() {
    return String.format(
        "healthy in-service datanodes (=%d) >= required datanodes (=%d)",
        nodeManager.getNodes(NodeStatus.inServiceHealthy()).size(), this.requiredDns);
  }

  @Override
  public void refresh(boolean forceRefresh) {
    // Do nothing.
    // As for this rule, there is nothing we read from SCM DB state and
    // validate it.
  }
}
