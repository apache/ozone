/*
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT;

/**
 * Class defining Safe mode exit criteria for Containers.
 */
public class ContainerSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport> {

  public static final Logger LOG = LoggerFactory.getLogger(ContainerSafeModeRule.class);
  private final ContainerManager containerManager;
  // Required cutoff % for containers with at least 1 reported replica.
  private final double safeModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private final Set<Long> ratisContainers;
  private final Set<Long> ecContainers;
  private final Map<Long, Set<UUID>> ecContainerDNsMap;
  private final AtomicLong ratisContainerWithMinReplicas = new AtomicLong(0);
  private final AtomicLong ecContainerWithMinReplicas = new AtomicLong(0);

  private double ratisMaxContainer;
  private double ecMaxContainer;

  public ContainerSafeModeRule(final String ruleName,
                               final EventQueue eventQueue,
                               final ConfigurationSource conf,
                               final ContainerManager containerManager,
                               final SCMSafeModeManager manager) {
    super(manager, ruleName, eventQueue);
    this.safeModeCutoff = getSafeModeCutoff(conf);
    this.containerManager = containerManager;
    this.ratisContainers = new HashSet<>();
    this.ecContainers = new HashSet<>();
    this.ecContainerDNsMap = new ConcurrentHashMap<>();
    initializeRule();
  }


  private static double getSafeModeCutoff(ConfigurationSource conf) {
    final double cutoff = conf.getDouble(HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);
    Preconditions.checkArgument((cutoff >= 0.0 && cutoff <= 1.0),
        HDDS_SCM_SAFEMODE_THRESHOLD_PCT  +
            " value should be >= 0.0 and <= 1.0");
    return cutoff;
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.CONTAINER_REGISTRATION_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    if (validateBasedOnReportProcessing()) {
      return (getCurrentContainerThreshold() >= safeModeCutoff) &&
          (getCurrentECContainerThreshold() >= safeModeCutoff);
    }

    // TODO: Split ContainerSafeModeRule into RatisContainerSafeModeRule and
    //   ECContainerSafeModeRule
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
    return ratisMaxContainer == 0 ? 1 :
        (ratisContainerWithMinReplicas.doubleValue() / ratisMaxContainer);
  }

  @VisibleForTesting
  public double getCurrentECContainerThreshold() {
    return ecMaxContainer == 0 ? 1 :
        (ecContainerWithMinReplicas.doubleValue() / ecMaxContainer);
  }


  // TODO: Report processing logic will be removed in future. HDDS-11958.
  @Override
  protected synchronized void process(
      final NodeRegistrationContainerReport reportsProto) {
    final DatanodeDetails datanodeDetails = reportsProto.getDatanodeDetails();
    final UUID datanodeUUID = datanodeDetails.getUuid();
    StorageContainerDatanodeProtocolProtos.ContainerReportsProto report = reportsProto.getReport();

    report.getReportsList().forEach(c -> {
      long containerID = c.getContainerID();


      // If it is a Ratis container.
      if (ratisContainers.contains(containerID)) {
        recordReportedContainer(containerID, Boolean.FALSE);
        ratisContainers.remove(containerID);
      }

      // If it is an EC container.
      if (ecContainers.contains(containerID)) {
        putInContainerDNsMap(containerID, ecContainerDNsMap, datanodeUUID);
        recordReportedContainer(containerID, Boolean.TRUE);
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [Ratis] have at least one"
          + " reported replica, {} % containers [EC] have at N reported replica.",
          getCurrentContainerThreshold() * 100, getCurrentECContainerThreshold() * 100);
    }
  }

  /**
   * Record the reported Container.
   *
   * We will differentiate and count according to the type of Container.
   *
   * @param containerID containerID
   * @param isEcContainer true, means ECContainer, false, means not ECContainer.
   */
  private void recordReportedContainer(long containerID, boolean isEcContainer) {

    int uuids = 1;
    if (isEcContainer && ecContainerDNsMap.containsKey(containerID)) {
      uuids = ecContainerDNsMap.get(containerID).size();
    }

    int minReplica = getMinReplica(containerID);
    if (uuids >= minReplica) {
      if (isEcContainer) {
        getSafeModeMetrics()
            .incCurrentContainersWithECDataReplicaReportedCount();
        ecContainerWithMinReplicas.getAndAdd(1);
      } else {
        ratisContainerWithMinReplicas.getAndAdd(1);
        getSafeModeMetrics()
            .incCurrentContainersWithOneReplicaReportedCount();
      }
    }
  }

  /**
   * Get the minimum replica.
   *
   * If it is a Ratis Contianer, the minimum copy is 1.
   * If it is an EC Container, the minimum copy will be the number of Data in replicationConfig.
   *
   * @param pContainerID containerID
   * @return MinReplica.
   */
  private int getMinReplica(long pContainerID) {

    try {
      ContainerID containerID = ContainerID.valueOf(pContainerID);
      ContainerInfo container = containerManager.getContainer(containerID);
      ReplicationConfig replicationConfig = container.getReplicationConfig();
      return replicationConfig.getMinimumNodes();
    } catch (ContainerNotFoundException e) {
      LOG.error("containerId = {} not found.", pContainerID, e);
    } catch (Exception e) {
      LOG.error("containerId = {} not found.", pContainerID, e);
    }

    return 1;
  }

  private void putInContainerDNsMap(long containerID, Map<Long, Set<UUID>> containerDNsMap,
      UUID datanodeUUID) {
    containerDNsMap.computeIfAbsent(containerID, key -> Sets.newHashSet());
    containerDNsMap.get(containerID).add(datanodeUUID);
  }

  @Override
  protected synchronized void cleanup() {
    ratisContainers.clear();
    ecContainers.clear();
    ecContainerDNsMap.clear();
  }

  @Override
  public String getStatusText() {

    // ratis container
    String status = String.format(
        "%1.2f%% of [Ratis] Containers(%s / %s) with at least one reported replica (=%1.2f) >= " +
        "safeModeCutoff (=%1.2f);",
        getCurrentContainerThreshold() * 100,
        ratisContainerWithMinReplicas, (long) ratisMaxContainer,
        getCurrentContainerThreshold(), this.safeModeCutoff);

    Set<Long> sampleRatisContainers = ratisContainers.stream().
        limit(SAMPLE_CONTAINER_DISPLAY_LIMIT).
        collect(Collectors.toSet());

    if (!sampleRatisContainers.isEmpty()) {
      String sampleContainerText =
          "Sample Ratis Containers not satisfying the criteria : " + sampleRatisContainers + ";";
      status = status.concat("\n").concat(sampleContainerText);
    }

    // ec container
    String ecStatus = String.format(
        "%1.2f%% of [EC] Containers(%s / %s) with at least N reported replica (=%1.2f) >= " +
        "safeModeCutoff (=%1.2f);",
        getCurrentECContainerThreshold() * 100,
        ecContainerWithMinReplicas, (long) ecMaxContainer,
        getCurrentECContainerThreshold(), this.safeModeCutoff);
    status = status.concat("\n").concat(ecStatus);

    Set<Long> sampleEcContainers = ecContainerDNsMap.entrySet().stream().
        filter(entry -> {
          Long containerId = entry.getKey();
          int minReplica = getMinReplica(containerId);
          Set<UUID> allReplicas = entry.getValue();
          if (allReplicas.size() >= minReplica) {
            return false;
          }
          return true;
        }).
        map(Map.Entry::getKey).
        limit(SAMPLE_CONTAINER_DISPLAY_LIMIT).
        collect(Collectors.toSet());

    if (!sampleEcContainers.isEmpty()) {
      String sampleECContainerText =
          "Sample EC Containers not satisfying the criteria : " + sampleEcContainers + ";";
      status = status.concat("\n").concat(sampleECContainerText);
    }

    return status;
  }


  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh || !validate()) {
      initializeRule();
    }
  }

  private boolean isClosed(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    return state == LifeCycleState.QUASI_CLOSED ||
        state == LifeCycleState.CLOSED;
  }

  private void initializeRule() {
    final List<ContainerInfo> containers = containerManager.getContainers();
    // Clean up the related data in the map.
    ratisContainers.clear();
    ecContainers.clear();

    // Iterate through the container list to
    // get the minimum replica count for each container.
    containers.forEach(container -> {
      // There can be containers in OPEN/CLOSING state which were never
      // created by the client. We are not considering these containers for
      // now. These containers can be handled by tracking pipelines.

      HddsProtos.ReplicationType replicationType = container.getReplicationType();

      if (isClosed(container) && container.getNumberOfKeys() > 0) {
        // If it's of type Ratis
        if (replicationType.equals(HddsProtos.ReplicationType.RATIS)) {
          ratisContainers.add(container.getContainerID());
        }

        // If it's of type EC
        if (replicationType.equals(HddsProtos.ReplicationType.EC)) {
          ecContainers.add(container.getContainerID());
        }
      }
    });

    ratisMaxContainer = ratisContainers.size();
    ecMaxContainer = ecContainers.size();

    long ratisCutOff = (long) Math.ceil(ratisMaxContainer * safeModeCutoff);
    long ecCutOff = (long) Math.ceil(ecMaxContainer * safeModeCutoff);

    getSafeModeMetrics().setNumContainerWithOneReplicaReportedThreshold(ratisCutOff);
    getSafeModeMetrics().setNumContainerWithECDataReplicaReportedThreshold(ecCutOff);

    LOG.info("Refreshed Containers with one replica threshold count {}, " +
        "with ec n replica threshold count {}.", ratisCutOff, ecCutOff);
  }
}
