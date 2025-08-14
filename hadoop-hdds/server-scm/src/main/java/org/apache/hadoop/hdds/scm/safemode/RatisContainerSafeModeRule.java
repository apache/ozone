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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Ratis Containers.
 * This rule validates that a configurable percentage of Ratis containers have a minimum
 * number of replicas reported by the DataNodes. This rule is not satisfied until this
 * condition is met.
 */
public class RatisContainerSafeModeRule extends AbstractContainerSafeModeRule {

  private static final Logger LOG = LoggerFactory.getLogger(RatisContainerSafeModeRule.class);
  private static final String NAME = "RatisContainerSafeModeRule";

  private final AtomicLong ratisContainerWithMinReplicas;

  public RatisContainerSafeModeRule(EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager manager) {
    super(conf, manager, containerManager, NAME, eventQueue, LOG);
    this.ratisContainerWithMinReplicas = new AtomicLong(0);
    initializeRule();
  }

  @Override
  protected ReplicationType getContainerType() {
    return ReplicationType.RATIS;
  }

  @Override
  protected void process(NodeRegistrationContainerReport report) {
    report.getReport().getReportsList().stream()
        .map(c -> ContainerID.valueOf(c.getContainerID()))
        .filter(getContainers()::remove)
        .forEach(c -> recordReportedContainer());

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % containers [Ratis] have at least one reported replica",
          String.format("%.2f", getCurrentContainerThreshold() * 100));
    }
  }

  /** Record the reported Container. */
  private void recordReportedContainer() {
    ratisContainerWithMinReplicas.incrementAndGet();
    getSafeModeMetrics().incCurrentContainersWithOneReplicaReportedCount();
  }

  @Override
  protected long getNumberOfContainersWithMinReplica() {
    return ratisContainerWithMinReplicas.longValue();
  }

  @Override
  protected Set<ContainerID> getSampleMissingContainers() {
    return getContainers().stream()
        .limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toSet());
  }

}
