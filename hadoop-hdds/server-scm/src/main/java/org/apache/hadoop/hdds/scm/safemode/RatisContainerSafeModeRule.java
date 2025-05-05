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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Ratis Containers.
 */
public class RatisContainerSafeModeRule extends SafeModeExitRule<NodeRegistrationContainerReport> {

  private static final Logger LOG = LoggerFactory.getLogger(RatisContainerSafeModeRule.class);
  private static final String NAME = "RatisContainerSafeModeRule";

  private final ContainerManager containerManager;
  // Required cutoff % for containers with at least 1 reported replica.
  private final double safeModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private final Set<Long> ratisContainers;
  private final AtomicLong ratisContainerWithMinReplicas;
  private double ratisMaxContainer;

  public RatisContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(manager, NAME, eventQueue);
    this.safeModeCutoff = getSafeModeCutoff(conf);
    this.containerManager = containerManager;
    this.ratisContainers = new HashSet<>();
    this.ratisContainerWithMinReplicas = new AtomicLong(0);
    initializeRule();
  }

  private static double getSafeModeCutoff(ConfigurationSource conf) {
    final double cutoff = conf.getDouble(HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    Preconditions.checkArgument((cutoff >= 0.0 && cutoff <= 1.0),
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT + " value should be >= 0.0 and <= 1.0");
    return cutoff;
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.CONTAINER_REGISTRATION_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    if (validateBasedOnReportProcessing()) {
      return (getCurrentContainerThreshold() >= safeModeCutoff);
    }

    final List<ContainerInfo> containers = containerManager.getContainers(
        ReplicationType.RATIS);

    return containers.stream()
        .filter(this::isClosed)
        .map(ContainerInfo::containerID)
        .noneMatch(this::isMissing);
  }

  /**
   * Checks if the container has any replica.
   */
  private boolean isMissing(ContainerID id) {
    try {
      return containerManager.getContainerReplicas(id).isEmpty();
    } catch (ContainerNotFoundException ex) {
      /*
       * This should never happen, in case this happens the container
       * somehow got removed from SCM.
       * Safemode rule doesn't have to log/fix this. We will just exclude this
       * from the rule validation.
       */
      return false;

    }
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return ratisMaxContainer == 0 ? 1 : (ratisContainerWithMinReplicas.doubleValue() / ratisMaxContainer);
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    report.getReport().getReportsList().forEach(c -> {
      long containerID = c.getContainerID();
      if (ratisContainers.contains(containerID)) {
        recordReportedContainer(containerID);
        ratisContainers.remove(containerID);
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [Ratis] have at least one reported replica",
          String.format("%.2f", getCurrentContainerThreshold() * 100));
    }
  }

  /**
   * Record the reported Container.
   *
   * @param containerID containerID
   */
  private void recordReportedContainer(long containerID) {
    ratisContainerWithMinReplicas.getAndAdd(1);
    getSafeModeMetrics()
        .incCurrentContainersWithOneReplicaReportedCount();
  }

  private void initializeRule() {
    ratisContainers.clear();
    containerManager.getContainers(ReplicationType.RATIS).stream()
        .filter(this::isClosed).filter(c -> c.getNumberOfKeys() > 0)
        .map(ContainerInfo::getContainerID).forEach(ratisContainers::add);
    ratisMaxContainer = ratisContainers.size();
    long ratisCutOff = (long) Math.ceil(ratisMaxContainer * safeModeCutoff);
    getSafeModeMetrics().setNumContainerWithOneReplicaReportedThreshold(ratisCutOff);

    LOG.info("Refreshed Containers with one replica threshold count {}.", ratisCutOff);
  }

  private boolean isClosed(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    return state == LifeCycleState.QUASI_CLOSED || state == LifeCycleState.CLOSED;
  }

  @Override
  public String getStatusText() {
    String status = String.format(
        "%1.2f%% of [Ratis] Containers(%s / %s) with at least one reported replica (=%1.2f) >= " +
            "safeModeCutoff (=%1.2f);",
        getCurrentContainerThreshold() * 100,
        ratisContainerWithMinReplicas, (long) ratisMaxContainer,
        getCurrentContainerThreshold(), this.safeModeCutoff);

    Set<Long> sampleRatisContainers = ratisContainers.stream().limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toSet());

    if (!sampleRatisContainers.isEmpty()) {
      String sampleContainerText = "Sample Ratis Containers not satisfying the criteria : " + sampleRatisContainers
          + ";";
      status = status.concat("\n").concat(sampleContainerText);
    }

    return status;
  }

  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh || !validate()) {
      initializeRule();
    }
  }

  @Override
  protected void cleanup() {
    ratisContainers.clear();
  }
}
