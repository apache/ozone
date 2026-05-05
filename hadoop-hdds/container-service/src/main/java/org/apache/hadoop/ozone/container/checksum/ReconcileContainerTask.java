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

package org.apache.hadoop.ozone.container.checksum;

import java.util.Objects;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to execute a container reconciliation task that has been queued from the ReplicationSupervisor.
 */
public class ReconcileContainerTask extends AbstractReplicationTask {
  private final ReconcileContainerCommand command;
  private final DNContainerOperationClient dnClient;
  private final ContainerController controller;
  public static final String METRIC_NAME = "ContainerReconciliations";
  public static final String METRIC_DESCRIPTION_SEGMENT = "Container Reconciliations";

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconcileContainerTask.class);

  public ReconcileContainerTask(ContainerController controller,
      DNContainerOperationClient dnClient, ReconcileContainerCommand command) {
    super(command.getContainerID(), command.getDeadline(), command.getTerm());
    this.command = command;
    this.controller = controller;
    this.dnClient = dnClient;
  }

  @Override
  public void runTask() {
    long start = Time.monotonicNow();

    LOG.info("{}", this);

    try {
      controller.reconcileContainer(dnClient, command.getContainerID(), command.getPeerDatanodes());
      setStatus(Status.DONE);
      long elapsed = Time.monotonicNow() - start;
      LOG.info("{} completed in {} ms", this, elapsed);
    } catch (Exception e) {
      long elapsed = Time.monotonicNow() - start;
      setStatus(Status.FAILED);
      LOG.warn("{} failed in {} ms", this, elapsed, e);
    }
  }

  @Override
  protected Object getCommandForDebug() {
    return command.toString();
  }

  @Override
  public String getMetricName() {
    return METRIC_NAME;
  }

  @Override
  public String getMetricDescriptionSegment() {
    return METRIC_DESCRIPTION_SEGMENT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReconcileContainerTask that = (ReconcileContainerTask) o;
    return Objects.equals(command, that.command);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId());
  }
}
