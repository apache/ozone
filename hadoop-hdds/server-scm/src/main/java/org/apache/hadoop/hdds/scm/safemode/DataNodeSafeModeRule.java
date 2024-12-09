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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_DATANODE_USE_PIPELINE_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_DATANODE_USE_PIPELINE_ENABLED_DEFAULT;

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

  // These are a set of new variables used for DN registration in Pipeline mode.
  private boolean usePipeline;
  private PipelineManager pipelineManager;
  private double dnReportedPercent;
  private Map<UUID, String> unRegisteredDn = new HashMap<>();

  public DataNodeSafeModeRule(String ruleName, EventQueue eventQueue,
      ConfigurationSource conf,
      SCMSafeModeManager manager, PipelineManager pipelineManager) {
    super(manager, ruleName, eventQueue);

    // The original strategy allows the user to set a minimum number of DNs to
    // ensure that the registered DNs in safe mode meet the expected quantity.
    this.requiredDns = conf.getInt(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE_DEFAULT);
    this.registeredDnSet = new HashSet<>(requiredDns * 2);

    // This is our newly added strategy,
    // which allows obtaining the DN list from the Pipeline list and calculates the
    // required number of DNs based on the configured ratio.
    this.usePipeline = conf.getBoolean(HDDS_SCM_SAFEMODE_DATANODE_USE_PIPELINE_ENABLED,
        HDDS_SCM_SAFEMODE_DATANODE_USE_PIPELINE_ENABLED_DEFAULT);
    if (this.usePipeline) {
      this.pipelineManager = pipelineManager;
      this.dnReportedPercent = conf.getDouble(
          HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT,
          HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT_DEFAULT);
      Preconditions.checkArgument((dnReportedPercent >= 0.0 && dnReportedPercent <= 1.0),
          HddsConfigKeys.HDDS_SCM_SAFEMODE_REPORTED_DATANODE_PCT  +
          " value should be >= 0.0 and <= 1.0");
    }

    initializeRule();
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
    registeredDnSet.add(dnUUID);
    registeredDns = registeredDnSet.size();
    if (registeredDnSet.contains(dnUUID)) {
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
    // If it is in Pipeline mode, we will attempt to print the unregistered DNs.
    if (this.usePipeline) {
      String status = String
          .format("Registered DataNodes (=%d) >= Required DataNodes (=%d)",
          this.registeredDns, this.requiredDns);

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
    return "";
  }

  @Override
  public void refresh(boolean forceRefresh) {
    initializeRule();
  }

  private void initializeRule() {
    if (usePipeline) {
      // We will attempt to retrieve the entire DN list here,
      // as the Pipeline only exists within the list of active DNs.
      Set<UUID> pipeLineDnSet = new HashSet<>();
      if (this.pipelineManager != null) {
        List<Pipeline> pipelines = this.pipelineManager.getPipelines();
        pipelines.forEach(pipeline -> {
          List<DatanodeDetails> nodes = pipeline.getNodes();
          for (DatanodeDetails node : nodes) {
            pipeLineDnSet.add(node.getUuid());
            // If the registeredDnSet does not contain this DN,
            // we will place this DN in the unRegisteredDn.
            if (!this.registeredDnSet.contains(node.getUuid())) {
              this.unRegisteredDn.put(node.getUuid(), node.getHostName());
            }
          }
        });
        this.requiredDns = (int) Math.ceil(dnReportedPercent * pipeLineDnSet.size());
        LOG.info("Total DataNodes is {} / RequiredDataNodes is {}, Reported Datanode count is {}.",
            pipeLineDnSet.size(), requiredDns, registeredDns);
      }
    }
  }
}
