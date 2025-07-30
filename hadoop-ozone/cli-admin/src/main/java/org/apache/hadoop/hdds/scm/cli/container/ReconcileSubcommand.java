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

package org.apache.hadoop.hdds.scm.cli.container;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 * Handle the container reconcile CLI command.
 */
@Command(
    name = "reconcile",
    description = "Reconcile container replicas",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconcileSubcommand extends ScmSubcommand {

  @CommandLine.Mixin
  private ContainerIDParameters containerList;

  @CommandLine.Option(names = { "--status" },
      defaultValue = "false",
      fallbackValue = "true",
      description = "Display the reconciliation status of this container's replicas")
  private boolean status;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (status) {
      executeStatus(scmClient);
    } else {
      executeReconcile(scmClient);
    }
  }

  private void executeStatus(ScmClient scmClient) throws IOException {
    // Do validation outside the json array writer, otherwise failed validation will print an empty json array.
    List<Long> containerIDs = containerList.getValidatedIDs();
    // Automatically creates one array for the output, while allowing us to flush each object individually.
    try (SequenceWriter arrayWriter = JsonUtils.getSequenceWriter(System.out)) {
      // Since status is retrieved using container info, do client side validation that it is only used for Ratis
      // containers. If EC containers are given, print a  message to stderr and eventually exit non-zero, but continue
      // processing the remaining containers.
      int failureCount = 0;
      for (Long containerID : containerIDs) {
        if (!printReconciliationStatus(scmClient, containerID, arrayWriter)) {
          failureCount++;
        }
      }
      if (failureCount > 0) {
        throw new RuntimeException("Failed to process reconciliation status for " + failureCount + " containers");
      }
    }
    // Array writer will not add a newline to the end.
    System.out.println();
  }

  private boolean printReconciliationStatus(ScmClient scmClient, long containerID, SequenceWriter arrayWriter) {
    try {
      ContainerInfo containerInfo = scmClient.getContainer(containerID);
      if (containerInfo.getReplicationType() != HddsProtos.ReplicationType.RATIS) {
        System.err.println("Cannot get status of container " + containerID +
            ". Reconciliation is only supported for Ratis replicated containers");
        return false;
      }
      List<ContainerReplicaInfo> replicas = scmClient.getContainerReplicas(containerID);
      arrayWriter.write(new ContainerWrapper(containerInfo, replicas));
      arrayWriter.flush();
    } catch (Exception ex) {
      System.err.println("Failed get reconciliation status of container " + containerID + ": " + ex.getMessage());
      return false;
    }
    return true;
  }

  private void executeReconcile(ScmClient scmClient) {
    int failureCount = 0;
    for (Long containerID : containerList.getValidatedIDs()) {
      try {
        scmClient.reconcileContainer(containerID);
        System.out.println("Reconciliation has been triggered for container " + (long) containerID);
        System.out.println("Use \"ozone admin container reconcile --status " + containerID + "\" to see the checksums of " +
            "each container replica");
      } catch (Exception ex) {
        System.err.println("Failed to trigger reconciliation for container " + containerID + ": " + ex.getMessage());
        failureCount++;
      }
    }
    if (failureCount > 0) {
      throw new RuntimeException("Failed trigger reconciliation for " + failureCount + " containers");
    }
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
    private int replicaIndex;
    @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
    private final long dataChecksum;

    ReplicaWrapper(ContainerReplicaInfo replica) {
      this.datanode = new DatanodeWrapper(replica.getDatanodeDetails());
      this.state = replica.getState();
      // Only display replica index when it has a positive value for EC.
      if (replica.getReplicaIndex() > 0) {
        this.replicaIndex = replica.getReplicaIndex();
      }
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
