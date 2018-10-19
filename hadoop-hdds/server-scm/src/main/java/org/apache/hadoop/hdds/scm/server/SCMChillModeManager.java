/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.server;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ScmOps;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer
    .NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageContainerManager enters chill mode on startup to allow system to
 * reach a stable state before becoming fully functional. SCM will wait
 * for certain resources to be reported before coming out of chill mode.
 *
 * ChillModeExitRule defines format to define new rules which must be satisfied
 * to exit Chill mode.
 * ContainerChillModeRule defines the only exit criteria right now.
 * On every new datanode registration event this class adds replicas
 * for reported containers and validates if cutoff threshold for
 * containers is meet.
 */
public class SCMChillModeManager implements
    EventHandler<NodeRegistrationContainerReport> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMChillModeManager.class);
  private final boolean isChillModeEnabled;
  private AtomicBoolean inChillMode = new AtomicBoolean(true);
  private AtomicLong containerWithMinReplicas = new AtomicLong(0);
  private Map<String, ChillModeExitRule> exitRules = new HashMap(1);
  private Configuration config;
  private static final String CONT_EXIT_RULE = "ContainerChillModeRule";
  private static final String DN_EXIT_RULE = "DataNodeChillModeRule";
  private final EventQueue eventPublisher;

  SCMChillModeManager(Configuration conf, List<ContainerInfo> allContainers,
      EventQueue eventQueue) {
    this.config = conf;
    this.eventPublisher = eventQueue;
    this.isChillModeEnabled = conf.getBoolean(
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_CHILLMODE_ENABLED_DEFAULT);
    if (isChillModeEnabled) {
      exitRules.put(CONT_EXIT_RULE,
          new ContainerChillModeRule(config, allContainers));
      exitRules.put(DN_EXIT_RULE, new DataNodeChillModeRule(config));
      emitChillModeStatus();
    } else {
      exitChillMode(eventQueue);
    }
  }

  /**
   * Emit Chill mode status.
   */
  @VisibleForTesting
  public void emitChillModeStatus() {
    eventPublisher.fireEvent(SCMEvents.CHILL_MODE_STATUS, getInChillMode());
  }

  private void validateChillModeExitRules(EventPublisher eventQueue) {
    for (ChillModeExitRule exitRule : exitRules.values()) {
      if (!exitRule.validate()) {
        return;
      }
    }
    exitChillMode(eventQueue);
  }

  /**
   * Exit chill mode. It does following actions:
   * 1. Set chill mode status to false.
   * 2. Emits START_REPLICATION for ReplicationManager.
   * 3. Cleanup resources.
   * 4. Emit chill mode status.
   * @param eventQueue
   */
  @VisibleForTesting
  public void exitChillMode(EventPublisher eventQueue) {
    LOG.info("SCM exiting chill mode.");
    setInChillMode(false);

    // TODO: Remove handler registration as there is no need to listen to
    // register events anymore.

    for (ChillModeExitRule e : exitRules.values()) {
      e.cleanup();
    }
    emitChillModeStatus();
  }

  @Override
  public void onMessage(
      NodeRegistrationContainerReport nodeRegistrationContainerReport,
      EventPublisher publisher) {
    if (getInChillMode()) {
      exitRules.get(CONT_EXIT_RULE).process(nodeRegistrationContainerReport);
      exitRules.get(DN_EXIT_RULE).process(nodeRegistrationContainerReport);
      validateChillModeExitRules(publisher);
    }
  }

  public boolean getInChillMode() {
    if (!isChillModeEnabled) {
      return false;
    }
    return inChillMode.get();
  }

  /**
   * Set chill mode status.
   */
  public void setInChillMode(boolean inChillMode) {
    this.inChillMode.set(inChillMode);
  }

  /**
   * Interface for defining chill mode exit rules.
   *
   * @param <T>
   */
  public interface ChillModeExitRule<T> {

    boolean validate();

    void process(T report);

    void cleanup();
  }

  /**
   * Class defining Chill mode exit criteria for Containers.
   */
  public class ContainerChillModeRule implements
      ChillModeExitRule<NodeRegistrationContainerReport> {

    // Required cutoff % for containers with at least 1 reported replica.
    private double chillModeCutoff;
    // Containers read from scm db (excluding containers in ALLOCATED state).
    private Map<Long, ContainerInfo> containerMap;
    private double maxContainer;

    public ContainerChillModeRule(Configuration conf,
        List<ContainerInfo> containers) {
      chillModeCutoff = conf
          .getDouble(HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT,
              HddsConfigKeys.HDDS_SCM_CHILLMODE_THRESHOLD_PCT_DEFAULT);
      containerMap = new ConcurrentHashMap<>();
      if(containers != null) {
        containers.forEach(c -> {
          // Containers in ALLOCATED state should not be included while
          // calculating the total number of containers here. They are not
          // reported by DNs and hence should not affect the chill mode exit
          // rule.
          if (c != null && c.getState() != null &&
              !c.getState().equals(HddsProtos.LifeCycleState.ALLOCATED)) {
            containerMap.put(c.getContainerID(), c);
          }
        });
        maxContainer = containerMap.size();
      }
    }

    @Override
    public boolean validate() {
      if (maxContainer == 0) {
        return true;
      }
      return getCurrentContainerThreshold() >= chillModeCutoff;
    }

    @VisibleForTesting
    public double getCurrentContainerThreshold() {
      if (maxContainer == 0) {
        return 1;
      }
      return (containerWithMinReplicas.doubleValue() / maxContainer);
    }

    @Override
    public void process(NodeRegistrationContainerReport reportsProto) {
      if (maxContainer == 0) {
        // No container to check.
        return;
      }

      reportsProto.getReport().getReportsList().forEach(c -> {
        if (containerMap.containsKey(c.getContainerID())) {
          if(containerMap.remove(c.getContainerID()) != null) {
            containerWithMinReplicas.getAndAdd(1);
          }
        }
      });
      if(getInChillMode()) {
        LOG.info("SCM in chill mode. {} % containers have at least one"
                + " reported replica.",
            (containerWithMinReplicas.get() / maxContainer) * 100);
      }
    }

    @Override
    public void cleanup() {
      containerMap.clear();
    }
  }

  /**
   * Class defining Chill mode exit criteria according to number of DataNodes
   * registered with SCM.
   */
  public class DataNodeChillModeRule implements
      ChillModeExitRule<NodeRegistrationContainerReport> {

    // Min DataNodes required to exit chill mode.
    private int requiredDns;
    private int registeredDns = 0;
    // Set to track registered DataNodes.
    private HashSet<UUID> registeredDnSet;

    public DataNodeChillModeRule(Configuration conf) {
      requiredDns = conf
          .getInt(HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE,
              HddsConfigKeys.HDDS_SCM_CHILLMODE_MIN_DATANODE_DEFAULT);
      registeredDnSet = new HashSet<>(requiredDns * 2);
    }

    @Override
    public boolean validate() {
      return registeredDns >= requiredDns;
    }

    @VisibleForTesting
    public double getRegisteredDataNodes() {
      return registeredDns;
    }

    @Override
    public void process(NodeRegistrationContainerReport reportsProto) {
      if (requiredDns == 0) {
        // No dn check required.
        return;
      }

      if(getInChillMode()) {
        registeredDnSet.add(reportsProto.getDatanodeDetails().getUuid());
        registeredDns = registeredDnSet.size();
        LOG.info("SCM in chill mode. {} DataNodes registered, {} required.",
            registeredDns, requiredDns);
      }
    }

    @Override
    public void cleanup() {
      registeredDnSet.clear();
    }
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }

  @VisibleForTesting
  public double getCurrentContainerThreshold() {
    return ((ContainerChillModeRule) exitRules.get(CONT_EXIT_RULE))
        .getCurrentContainerThreshold();
  }

  /**
   * Operations restricted in SCM chill mode.
   */
  public static class ChillModeRestrictedOps {
    private static EnumSet restrictedOps =  EnumSet.noneOf(ScmOps.class);

    static {
      restrictedOps.add(ScmOps.allocateBlock);
      restrictedOps.add(ScmOps.allocateContainer);
    }

    public static boolean isRestrictedInChillMode(ScmOps opName) {
      return restrictedOps.contains(opName);
    }
  }

}
