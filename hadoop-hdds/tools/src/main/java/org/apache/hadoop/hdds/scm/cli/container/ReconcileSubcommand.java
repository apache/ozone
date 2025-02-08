/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;

import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handle the container reconcile command.
 */
@Command(
    name = "reconcile",
    description = "Reconcile container replicas",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconcileSubcommand extends ScmSubcommand {

  @CommandLine.Parameters(description = "ID of the container to reconcile")
  private long containerId;

  @CommandLine.Option(names = { "--status" },
      defaultValue = "false",
      fallbackValue = "true",
      description = "Display the reconciliation status of this container's replicas")
  private boolean status;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (status) {
      printReconciliationStatus(scmClient);
    } else {
      executeReconciliation(scmClient);
    }
  }

  private void printReconciliationStatus(ScmClient scmClient) throws IOException {
    ContainerInfo containerInfo = scmClient.getContainer(containerId);
    if (containerInfo.getReplicationType() != HddsProtos.ReplicationType.RATIS) {
      throw new RuntimeException("Reconciliation is only supported for Ratis replicated containers");
    }
    List<ContainerReplicaInfo> replicas = scmClient.getContainerReplicas(containerId);
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(
        new ContainerWrapper(containerInfo, replicas)));
  }

  private void executeReconciliation(ScmClient scmClient) throws IOException {
    scmClient.reconcileContainer(containerId);
    System.out.println("Reconciliation has been triggered for container " + containerId);
    System.out.println("Use \"ozone admin container reconcile --status " + containerId + "\" to see the checksums of " +
        "each container replica");
  }

  /**
   * Used to json serialize the container and replica information for output.
   */
  private static class ContainerWrapper {
    private final long containerID;
    private final HddsProtos.LifeCycleState state;
    private final ReplicationConfig replicationConfig;
    private boolean replicasMatch;
    private final List<ReplicaWrapper> replicas;

    ContainerWrapper(ContainerInfo info, List<ContainerReplicaInfo> replicas) {
      this.containerID = info.getContainerID();
      this.state = info.getState();
      this.replicationConfig = info.getReplicationConfig();

      this.replicas = new ArrayList<>();
      this.replicasMatch = true;
      long firstChecksum = 0;
      if (!replicas.isEmpty()) {
        firstChecksum = replicas.get(0).getDataChecksum();
      }
      for (ContainerReplicaInfo replica: replicas) {
        replicasMatch = replicasMatch && (firstChecksum == replica.getDataChecksum());
        this.replicas.add(new ReplicaWrapper(replica));
      }
    }

    public long getContainerID() {
      return containerID;
    }

    public HddsProtos.LifeCycleState getState() {
      return state;
    }

    public ReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    public boolean getReplicasMatch() {
      return replicasMatch;
    }

    public List<ReplicaWrapper> getReplicas() {
      return replicas;
    }
  }

  private static class ReplicaWrapper {
    private final DatanodeWrapper datanode;
    private final String state;
    private final int replicaIndex;
    private final long dataChecksum;

    ReplicaWrapper(ContainerReplicaInfo replica) {
      this.datanode = new DatanodeWrapper(replica.getDatanodeDetails());
      this.state = replica.getState();
      this.replicaIndex = replica.getReplicaIndex();
      this.dataChecksum = replica.getDataChecksum();
    }

    public DatanodeWrapper getDatanode() {
      return datanode;
    }

    public String getState() {
      return state;
    }

    /**
     * Replica index is only included in the output if it is non-zero, which will be the case for EC.
     * For Ratis, avoid printing all zero replica indices to avoid confusion.
     */
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getReplicaIndex() {
      return replicaIndex;
    }

    public long getDataChecksum() {
      return dataChecksum;
    }
  }

  private static class DatanodeWrapper {
    private final String hostname;
    private final String uuid;

    DatanodeWrapper(DatanodeDetails dnDetails) {
      this.hostname = dnDetails.getHostName();
      this.uuid = dnDetails.getUuidString();
    }

    public String getHostname() {
      return hostname;
    }

    public String getUuid() {
      return uuid;
    }
  }
}
