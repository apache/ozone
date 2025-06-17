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
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
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
 * Safe mode rule for EC containers.
 */
public class ECContainerSafeModeRule extends SafeModeExitRule<NodeRegistrationContainerReport> {

  private static final Logger LOG = LoggerFactory.getLogger(ECContainerSafeModeRule.class);
  private static final String NAME = "ECContainerSafeModeRule";
  private static final int DEFAULT_MIN_REPLICA = 1;

  private final ContainerManager containerManager;
  private final double safeModeCutoff;
  private final Set<Long> ecContainers;
  private final Map<Long, Set<UUID>> ecContainerDNsMap;
  private final AtomicLong ecContainerWithMinReplicas;
  private double ecMaxContainer;

  public ECContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(manager, NAME, eventQueue);
    this.safeModeCutoff = getSafeModeCutoff(conf);
    this.containerManager = containerManager;
    this.ecContainers = new HashSet<>();
    this.ecContainerDNsMap = new ConcurrentHashMap<>();
    this.ecContainerWithMinReplicas = new AtomicLong(0);
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
      return getCurrentContainerThreshold() >= safeModeCutoff;
    }

    final List<ContainerInfo> containers = containerManager.getContainers(
        ReplicationType.EC);

    return containers.stream()
        .filter(this::isClosed)
        .map(ContainerInfo::containerID)
        .noneMatch(this::isMissing);
  }

  /**
   * Checks if the container has at least the minimum required number of replicas.
   */
  private boolean isMissing(ContainerID id) {
    try {
      int minReplica = getMinReplica(id.getId());
      return containerManager.getContainerReplicas(id).size() < minReplica;
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
    return ecMaxContainer == 0 ? 1 : (ecContainerWithMinReplicas.doubleValue() / ecMaxContainer);
  }

  /**
   * Get the minimum replica.
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
    } catch (Exception e) {
      LOG.error("containerId = {} not found.", pContainerID, e);
    }

    return DEFAULT_MIN_REPLICA;
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    DatanodeDetails datanodeDetails = report.getDatanodeDetails();
    UUID datanodeUUID = datanodeDetails.getUuid();

    report.getReport().getReportsList().forEach(c -> {
      long containerID = c.getContainerID();
      if (ecContainers.contains(containerID)) {
        putInContainerDNsMap(containerID, ecContainerDNsMap, datanodeUUID);
        recordReportedContainer(containerID);
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [EC] have at N reported replica",
          getCurrentContainerThreshold() * 100);
    }
  }

  private void putInContainerDNsMap(long containerID,
      Map<Long, Set<UUID>> containerDNsMap,
      UUID datanodeUUID) {
    containerDNsMap.computeIfAbsent(containerID, key -> Sets.newHashSet()).add(datanodeUUID);
  }

  /**
   * Record the reported Container.
   *
   * @param containerID containerID
   */
  private void recordReportedContainer(long containerID) {

    int uuids = 1;
    if (ecContainerDNsMap.containsKey(containerID)) {
      uuids = ecContainerDNsMap.get(containerID).size();
    }

    int minReplica = getMinReplica(containerID);
    if (uuids >= minReplica) {
      getSafeModeMetrics()
          .incCurrentContainersWithECDataReplicaReportedCount();
      ecContainerWithMinReplicas.getAndAdd(1);
    }
  }

  private void initializeRule() {
    ecContainers.clear();
    containerManager.getContainers(ReplicationType.EC).stream()
        .filter(this::isClosed).filter(c -> c.getNumberOfKeys() > 0)
        .map(ContainerInfo::getContainerID).forEach(ecContainers::add);
    ecMaxContainer = ecContainers.size();
    long ecCutOff = (long) Math.ceil(ecMaxContainer * safeModeCutoff);
    getSafeModeMetrics().setNumContainerWithECDataReplicaReportedThreshold(ecCutOff);

    LOG.info("Refreshed Containers with ec n replica threshold count {}.", ecCutOff);
  }

  private boolean isClosed(ContainerInfo container) {
    final LifeCycleState state = container.getState();
    return state == LifeCycleState.QUASI_CLOSED || state == LifeCycleState.CLOSED;
  }

  @Override
  public String getStatusText() {
    String status = String.format(
        "%1.2f%% of [EC] Containers(%s / %s) with at least N reported replica (=%1.2f) >= " +
            "safeModeCutoff (=%1.2f);",
        getCurrentContainerThreshold() * 100,
        ecContainerWithMinReplicas, (long) ecMaxContainer,
        getCurrentContainerThreshold(), this.safeModeCutoff);

    Set<Long> sampleEcContainers = ecContainerDNsMap.entrySet().stream().filter(entry -> {
      Long containerId = entry.getKey();
      int minReplica = getMinReplica(containerId);
      Set<UUID> allReplicas = entry.getValue();
      return allReplicas.size() < minReplica;
    }).map(Map.Entry::getKey).limit(SAMPLE_CONTAINER_DISPLAY_LIMIT).collect(Collectors.toSet());

    if (!sampleEcContainers.isEmpty()) {
      String sampleECContainerText = "Sample EC Containers not satisfying the criteria : " + sampleEcContainers + ";";
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

  @Override
  protected void cleanup() {
    ecContainers.clear();
    ecContainerDNsMap.clear();
  }
}
