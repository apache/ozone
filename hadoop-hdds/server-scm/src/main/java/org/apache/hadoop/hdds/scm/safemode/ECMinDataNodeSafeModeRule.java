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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmUtils;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

/**
 * Safe mode exit rule for EC-default clusters.
 *
 * <p>EC pipelines are ephemeral and created on demand. This rule ensures that
 * at least {@code data + parity} healthy DataNodes are available before SCM
 * exits safe mode for EC-default clusters.
 *
 * <p>For non-EC defaults this rule is a no-op.
 */
public class ECMinDataNodeSafeModeRule
    extends SafeModeExitRule<NodeRegistrationContainerReport> {

  private final boolean enabled;
  private final int requiredDns;
  private final String ecConfigLabel;
  private final NodeManager nodeManager;
  private final Set<DatanodeID> registeredDnSet;

  public ECMinDataNodeSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      NodeManager nodeManager,
      SCMSafeModeManager safeModeManager) {
    super(safeModeManager, eventQueue);
    this.nodeManager = nodeManager;

    ReplicationConfig defaultConfig = ScmUtils.getDefaultReplicationConfig(
        conf, SCMSafeModeManager.getLogger(),
        ECMinDataNodeSafeModeRule.class.getSimpleName());
    if (defaultConfig != null
        && defaultConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
      ECReplicationConfig ecConfig = (ECReplicationConfig) defaultConfig;
      this.requiredDns = ecConfig.getRequiredNodes();
      this.ecConfigLabel = ecConfig.configFormat();
      this.enabled = true;
      this.registeredDnSet = new HashSet<>(Math.max(requiredDns * 2, 1));
      SCMSafeModeManager.getLogger().info(
          "ECMinDataNodeSafeModeRule enabled for default EC config {}. "
              + "Required healthy DataNodes for safemode exit: {}.",
          ecConfigLabel, requiredDns);
    } else {
      this.requiredDns = 0;
      this.ecConfigLabel = "";
      this.enabled = false;
      this.registeredDnSet = new HashSet<>(0);
      SCMSafeModeManager.getLogger().debug(
          "ECMinDataNodeSafeModeRule disabled: default replication is not EC.");
    }
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.NODE_REGISTRATION_CONT_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    if (!enabled) {
      return true;
    }
    if (validateBasedOnReportProcessing()) {
      return getRegisteredDns() >= requiredDns;
    }
    return nodeManager.getNodes(NodeStatus.inServiceHealthy()).size() >= requiredDns;
  }

  @Override
  protected synchronized void process(NodeRegistrationContainerReport report) {
    if (!enabled) {
      return;
    }
    DatanodeID dnId = report.getDatanodeDetails().getID();
    if (registeredDnSet.add(dnId)) {
      if (scmInSafeMode()) {
        SCMSafeModeManager.getLogger().info(
            "SCM in safe mode. EC rule progress: {} of {} required "
                + "DataNodes registered for EC {}.",
            getRegisteredDns(), requiredDns, ecConfigLabel);
      }
    }
  }

  @Override
  protected synchronized void cleanup() {
    registeredDnSet.clear();
  }

  @Override
  public synchronized String getStatusText() {
    if (!enabled) {
      return "ECMinDataNodeSafeModeRule is not applicable "
          + "(default replication is not EC)";
    }
    return String.format(
        "EC (%s) safemode: registered DataNodes (=%d) >= required DataNodes (=%d)",
        ecConfigLabel, getRegisteredDns(), requiredDns);
  }

  @Override
  public void refresh(boolean forceRefresh) {
    // Nothing to refresh from SCM DB for this rule.
  }

  @VisibleForTesting
  int getRequiredDns() {
    return requiredDns;
  }

  @VisibleForTesting
  synchronized int getRegisteredDns() {
    return registeredDnSet.size();
  }

  boolean isEnabled() {
    return enabled;
  }
}
