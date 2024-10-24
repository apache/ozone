/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.safemode;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria according to number of DataNodes
 * registered with SCM.
 */
public class DataNodeSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataNodeSafeModeRule.class);

  // Min DataNodes required to exit safe mode.
  private int requiredDns;
  private int registeredDns = 0;
  // Set to track registered DataNodes.
  private HashSet<UUID> registeredDnSet;
  private PipelineManager pipelineManager;
  private Set<UUID> pipeLineDnSet = new HashSet<>();
  private Map<UUID, String> unRegisteredDn = new HashMap<>();
  private final double dnReportedPercent;

  public DataNodeSafeModeRule(String ruleName, EventQueue eventQueue,
      ConfigurationSource conf,
      SCMSafeModeManager manager, PipelineManager pipelineManager) {
    super(manager, ruleName, eventQueue);

    this.requiredDns = conf.getInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE_DEFAULT);
    this.registeredDnSet = new HashSet<>(requiredDns * 2);
    this.pipelineManager = pipelineManager;
    this.dnReportedPercent = conf.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT_DEFAULT);

    Preconditions.checkArgument((dnReportedPercent >= 0.0 && dnReportedPercent <= 1.0),
        HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT  +
         " value should be >= 0.0 and <= 1.0");

    initializeRule(false);
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.NODE_REGISTRATION_CONT_REPORT;
  }

  @Override
  protected boolean validate() {
    return registeredDns >= requiredDns;
  }

  @Override
  protected void process(NodeRegistrationContainerReport reportsProto) {
    UUID dnUUID = reportsProto.getDatanodeDetails().getUuid();

    // If a DN is registered for the first time
    // (as it is possible for the DN to be registered multiple times),
    // we will write the UUID of this DN into the `registeredDnSet` and
    // remove it from the unregistered list.
    if (pipeLineDnSet.contains(dnUUID) || !registeredDnSet.contains(dnUUID)) {
      registeredDnSet.add(dnUUID);
      registeredDns = registeredDnSet.size();
      unRegisteredDn.remove(dnUUID);
    }

    // Print the DN registration logs.
    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().debug(
          "SCM in safe mode. {} DataNodes registered, {} required.", registeredDns, requiredDns);
    }
  }

  @Override
  protected void cleanup() {
    registeredDnSet.clear();
    unRegisteredDn.clear();
  }

  @Override
  public String getStatusText() {

    String status = String
        .format("Registered DataNodes (=%d) >= Required DataNodes (=%d) / Total DataNode (%d); ",
        this.registeredDns, this.requiredDns, this.pipeLineDnSet.size());

    // Retrieve the list of unregistered DNs.
    List<String> unRegisteredDnHostNames = unRegisteredDn.values()
        .stream()
        .limit(SAMPLE_DN_DISPLAY_LIMIT)
        .collect(Collectors.toList());

    // We will concatenate the information of unregistered DNs and then display it.
    if (!unRegisteredDnHostNames.isEmpty()) {
      String sampleDNText = "Unregistered DN : " + unRegisteredDnHostNames;
      status = status.concat("\n").concat(sampleDNText);
    }

    return status;
  }


  @Override
  public void refresh(boolean forceRefresh) {
    if (forceRefresh) {
      initializeRule(true);
    } else {
      if (!validate()) {
        initializeRule(true);
      }
    }
  }

  private void initializeRule(boolean refresh) {
    // We will attempt to retrieve the entire DN list here,
    // as the Pipeline only exists within the list of active DNs.
    if (pipelineManager != null) {
      List<Pipeline> pipelines = pipelineManager.getPipelines();
      pipelines.forEach(pipeline -> {
        List<DatanodeDetails> nodes = pipeline.getNodes();
        for (DatanodeDetails node : nodes) {
          pipeLineDnSet.add(node.getUuid());
          // If the registeredDnSet does not contain this DN,
          // we will place this DN in the unRegisteredDn.
          if (!registeredDnSet.contains(node.getUuid())) {
            unRegisteredDn.put(node.getUuid(), node.getHostName());
          }
        }
      });
      requiredDns = (int) Math.ceil(dnReportedPercent * pipeLineDnSet.size());
    }

    String totalDataNode = pipeLineDnSet.size() > 0 ?
        String.valueOf(pipeLineDnSet.size()) : "UNKNOW";

    if (refresh) {
      LOG.info("Refreshed Total DataNode count is {} / threshold = {}, datanode reported count is {}.",
          totalDataNode, requiredDns, registeredDns);
    } else {
      LOG.info("Total DataNode count is {} / threshold = {}, datanode reported count is {}.",
          totalDataNode, requiredDns, registeredDns);
    }
  }
}
