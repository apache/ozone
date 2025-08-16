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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Safe mode rule for EC containers.
 * This rule validates that a configurable percentage of EC containers have a minimum
 * number of replicas reported by the DataNodes. This rule is not satisfied until this
 * condition is met.
 */
public class ECContainerSafeModeRule extends AbstractContainerSafeModeRule {

  private static final Logger LOG = LoggerFactory.getLogger(ECContainerSafeModeRule.class);
  private static final String NAME = "ECContainerSafeModeRule";

  private final Map<ContainerID, Set<DatanodeID>> ecContainerDNsMap;
  private final AtomicLong ecContainerWithMinReplicas;

  public ECContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(conf, manager, containerManager, NAME, eventQueue, LOG);
    this.ecContainerDNsMap = new ConcurrentHashMap<>();
    this.ecContainerWithMinReplicas = new AtomicLong(0);
    initializeRule();
  }

  @Override
  protected ReplicationType getContainerType() {
    return ReplicationType.EC;
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    final DatanodeID datanodeID = report.getDatanodeDetails().getID();

    report.getReport().getReportsList().stream()
        .map(c -> ContainerID.valueOf(c.getContainerID()))
        .filter(getContainers()::contains)
        .forEach(containerID -> {
          putInContainerDNsMap(containerID, ecContainerDNsMap, datanodeID);
          recordReportedContainer(containerID);
        });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [EC] have at N reported replica",
          getCurrentContainerThreshold() * 100);
    }
  }

  private void putInContainerDNsMap(ContainerID containerID,
      Map<ContainerID, Set<DatanodeID>> containerDNsMap,
      DatanodeID datanodeID) {
    containerDNsMap.computeIfAbsent(containerID, key -> Sets.newHashSet()).add(datanodeID);
  }

  /**
   * Record the reported Container.
   *
   * @param containerID containerID
   */
  private void recordReportedContainer(ContainerID containerID) {
    final int minReplica = getMinReplica(containerID);
    final int noOfDNs = ecContainerDNsMap.getOrDefault(containerID, Collections.emptySet()).size();
    if (noOfDNs >= minReplica) {
      getSafeModeMetrics().incCurrentContainersWithECDataReplicaReportedCount();
      ecContainerWithMinReplicas.getAndAdd(1);
    }
  }

  @Override
  protected long getNumberOfContainersWithMinReplica() {
    return ecContainerWithMinReplicas.longValue();
  }

  @Override
  protected Set<ContainerID> getSampleMissingContainers() {
    return ecContainerDNsMap.entrySet().stream()
        .filter(entry -> entry.getValue().size() < getMinReplica(entry.getKey()))
        .map(Map.Entry::getKey)
        .limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toSet());
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    ecContainerDNsMap.clear();
  }
}
