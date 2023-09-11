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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Containers.
 */
public class ContainerSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport> {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerSafeModeRule.class);
  // Required cutoff % for containers with at least 1 reported replica.
  private double safeModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private Map<Long, ContainerInfo> containerMap;
  private double maxContainer;

  private AtomicLong containerWithMinReplicas = new AtomicLong(0);
  private final ContainerManager containerManager;

  public ContainerSafeModeRule(String ruleName, EventQueue eventQueue,
             ConfigurationSource conf,
             List<ContainerInfo> containers,
             ContainerManager containerManager, SCMSafeModeManager manager) {
    super(manager, ruleName, eventQueue);
    this.containerManager = containerManager;
    safeModeCutoff = conf.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);

    Preconditions.checkArgument(
        (safeModeCutoff >= 0.0 && safeModeCutoff <= 1.0),
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT  +
            " value should be >= 0.0 and <= 1.0");

    containerMap = new ConcurrentHashMap<>();
    containers.forEach(container -> {
      // There can be containers in OPEN/CLOSING state which were never
      // created by the client. We are not considering these containers for
      // now. These containers can be handled by tracking pipelines.

      Optional.ofNullable(container.getState())
          .filter(state -> (state == HddsProtos.LifeCycleState.QUASI_CLOSED ||
              state == HddsProtos.LifeCycleState.CLOSED)
              && container.getNumberOfKeys() > 0)
          .ifPresent(s -> containerMap.put(container.getContainerID(),
              container));
    });
    maxContainer = containerMap.size();
    long cutOff = (long) Math.ceil(maxContainer * safeModeCutoff);
    getSafeModeMetrics().setNumContainerWithOneReplicaReportedThreshold(cutOff);

    LOG.info("containers with one replica threshold count {}", cutOff);
  }


  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.NODE_REGISTRATION_CONT_REPORT;
  }


  @Override
  protected synchronized boolean validate() {
    return getCurrentContainerThreshold() >= safeModeCutoff;
  }

  @VisibleForTesting
  public synchronized double getCurrentContainerThreshold() {
    if (maxContainer == 0) {
      return 1;
    }
    return (containerWithMinReplicas.doubleValue() / maxContainer);
  }

  @Override
  protected synchronized void process(
      NodeRegistrationContainerReport reportsProto) {

    reportsProto.getReport().getReportsList().forEach(c -> {
      if (containerMap.containsKey(c.getContainerID())) {
        if (containerMap.remove(c.getContainerID()) != null) {
          containerWithMinReplicas.getAndAdd(1);
          getSafeModeMetrics()
              .incCurrentContainersWithOneReplicaReportedCount();
        }
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers have at least one"
              + " reported replica.",
          (containerWithMinReplicas.doubleValue() / maxContainer) * 100);
    }
  }

  @Override
  protected synchronized void cleanup() {
    containerMap.clear();
  }

  @Override
  public String getStatusText() {
    return String
        .format(
            "%% of containers with at least one reported replica (=%1.2f) >= "
                + "safeModeCutoff (=%1.2f)",
            getCurrentContainerThreshold(), this.safeModeCutoff);
  }


  @Override
  public synchronized void refresh(boolean forceRefresh) {
    if (forceRefresh) {
      reInitializeRule();
    } else {
      if (!validate()) {
        reInitializeRule();
      }
    }
  }

  private void reInitializeRule() {
    containerMap.clear();
    containerManager.getContainers().forEach(container -> {
      // There can be containers in OPEN/CLOSING state which were never
      // created by the client. We are not considering these containers for
      // now. These containers can be handled by tracking pipelines.

      Optional.ofNullable(container.getState())
          .filter(state -> (state == HddsProtos.LifeCycleState.QUASI_CLOSED ||
              state == HddsProtos.LifeCycleState.CLOSED)
              && container.getNumberOfKeys() > 0)
          .ifPresent(s -> containerMap.put(container.getContainerID(),
              container));
    });

    maxContainer = containerMap.size();
    long cutOff = (long) Math.ceil(maxContainer * safeModeCutoff);

    LOG.info("Refreshed one replica container threshold {}, " +
        "currentThreshold {}", cutOff, containerWithMinReplicas.get());
    getSafeModeMetrics()
        .setNumContainerWithOneReplicaReportedThreshold(cutOff);
  }

}
