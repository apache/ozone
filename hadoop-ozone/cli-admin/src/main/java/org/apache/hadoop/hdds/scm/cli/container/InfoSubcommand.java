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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * This is the handler that process container info command.
 */
@Command(
    name = "info",
    description = "Show information about a specific container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class InfoSubcommand extends ScmSubcommand {

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @CommandLine.Mixin
  private ContainerIDParameters containerList;

  private boolean multiContainer = false;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    // validate all container IDs and fail fast
    List<Long> containerIDs = containerList.getValidatedIDs();

    boolean first = true;
    multiContainer = containerIDs.size() > 1;

    printHeader();
    for (Long containerID : containerIDs) {
      if (!first) {
        printBreak();
      }
      printDetails(scmClient, containerID);
      first = false;
    }
    printFooter();
  }

  private void printHeader() {
    if (json && multiContainer) {
      System.out.println("[");
    }
  }

  private void printFooter() {
    if (json && multiContainer) {
      System.out.println("]");
    }
  }

  private void printError(String error) {
    System.err.println(error);
  }

  private void printBreak() {
    if (json) {
      System.out.println(",");
    } else {
      System.out.println("");
    }
  }

  private void printDetails(ScmClient scmClient, long containerID) throws IOException {
    final ContainerWithPipeline container;
    try {
      container = scmClient.getContainerWithPipeline(containerID);
      Objects.requireNonNull(container, "Container cannot be null");
    } catch (IOException e) {
      printError("Unable to retrieve the container details for " + containerID);
      return;
    }

    List<ContainerReplicaInfo> replicas = null;
    try {
      replicas = scmClient.getContainerReplicas(containerID);
    } catch (IOException e) {
      printError("Unable to retrieve the replica details: " + e.getMessage());
    }

    if (json) {
      if (!container.getPipeline().isEmpty()) {
        ContainerWithPipelineAndReplicas wrapper =
            new ContainerWithPipelineAndReplicas(container.getContainerInfo(),
                container.getPipeline(), replicas,
                container.getContainerInfo().getPipelineID());
        System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(wrapper));
      } else {
        ContainerWithoutDatanodes wrapper =
            new ContainerWithoutDatanodes(container.getContainerInfo(),
                container.getPipeline(), replicas,
                container.getContainerInfo().getPipelineID());
        System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(wrapper));
      }
    } else {
      // Print container report info.
      System.out.printf("Container id: %s%n", containerID);
      if (isVerbose()) {
        System.out.printf("Pipeline Info: %s%n", container.getPipeline());
      } else {
        System.out.printf("Pipeline id: %s%n", container.getPipeline().getId().getId());
      }
      System.out.printf("Write PipelineId: %s%n",
          container.getContainerInfo().getPipelineID().getId());
      try {
        String pipelineState = scmClient.getPipeline(
                container.getContainerInfo().getPipelineID().getProtobuf())
            .getPipelineState().toString();
        System.out.printf("Write Pipeline State: %s%n", pipelineState);
      } catch (IOException ioe) {
        if (SCMHAUtils.unwrapException(
            ioe) instanceof PipelineNotFoundException) {
          System.out.println("Write Pipeline State: CLOSED");
        } else {
          printError("Failed to retrieve pipeline info");
        }
      }
      System.out.printf("Container State: %s%n", container.getContainerInfo().getState());

      // Print pipeline of an existing container.
      String machinesStr = container.getPipeline().getNodes().stream().map(
              InfoSubcommand::buildDatanodeDetails)
          .collect(Collectors.joining(",\n"));
      System.out.printf("Datanodes: [%s]%n", machinesStr);

      // Print the replica details if available
      if (replicas != null) {
        String replicaStr = replicas.stream()
            .sorted(Comparator.comparing(ContainerReplicaInfo::getReplicaIndex))
            .map(InfoSubcommand::buildReplicaDetails)
            .collect(Collectors.joining(",\n"));
        System.out.printf("Replicas: [%s]%n", replicaStr);
      }
    }
  }

  private static String buildDatanodeDetails(DatanodeDetails details) {
    return details.getUuidString() + "/" + details.getHostName();
  }

  private static String buildReplicaDetails(ContainerReplicaInfo replica) {
    StringBuilder sb = new StringBuilder()
        .append("State: ").append(replica.getState()).append(';');
    if (replica.getReplicaIndex() != -1) {
      sb.append(" ReplicaIndex: ").append(replica.getReplicaIndex()).append(';');
    }
    sb.append(" Origin: ").append(replica.getPlaceOfBirth().toString()).append(';')
        .append(" Location: ").append(buildDatanodeDetails(replica.getDatanodeDetails()));
    return sb.toString();
  }

  private static class ContainerWithPipelineAndReplicas {

    private ContainerInfo containerInfo;
    private Pipeline pipeline;
    private List<ContainerReplicaInfo> replicas;
    private PipelineID writePipelineID;

    ContainerWithPipelineAndReplicas(ContainerInfo container, Pipeline pipeline,
        List<ContainerReplicaInfo> replicas, PipelineID pipelineID) {
      this.containerInfo = container;
      this.pipeline = pipeline;
      this.replicas = replicas;
      this.writePipelineID = pipelineID;
    }

    public ContainerInfo getContainerInfo() {
      return containerInfo;
    }

    public Pipeline getPipeline() {
      return pipeline;
    }

    public List<ContainerReplicaInfo> getReplicas() {
      return replicas;
    }

    public PipelineID getWritePipelineID() {
      return writePipelineID;
    }

  }

  private static class ContainerWithoutDatanodes {

    private ContainerInfo containerInfo;
    private PipelineWithoutDatanodes pipeline;
    private List<ContainerReplicaInfo> replicas;
    private PipelineID writePipelineId;

    ContainerWithoutDatanodes(ContainerInfo container, Pipeline pipeline,
        List<ContainerReplicaInfo> replicas, PipelineID pipelineID) {
      this.containerInfo = container;
      this.pipeline = new PipelineWithoutDatanodes(pipeline);
      this.replicas = replicas;
      this.writePipelineId = pipelineID;
    }

    public ContainerInfo getContainerInfo() {
      return containerInfo;
    }

    public PipelineWithoutDatanodes getPipeline() {
      return pipeline;
    }

    public List<ContainerReplicaInfo> getReplicas() {
      return replicas;
    }

    public PipelineID getWritePipelineId() {
      return writePipelineId;
    }
  }

  // All Pipeline information except the ones dependent on datanodes
  private static final class PipelineWithoutDatanodes {
    private final PipelineID id;
    private final ReplicationConfig replicationConfig;
    private final Pipeline.PipelineState state;
    private Instant creationTimestamp;
    private Map<DatanodeDetails, Long> nodeStatus;

    private PipelineWithoutDatanodes(Pipeline pipeline) {
      this.id = pipeline.getId();
      this.replicationConfig = pipeline.getReplicationConfig();
      this.state = pipeline.getPipelineState();
      this.creationTimestamp = pipeline.getCreationTimestamp();
      this.nodeStatus = new HashMap<>(); // All DNs down
    }

    public PipelineID getId() {
      return id;
    }

    public ReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    public Pipeline.PipelineState getPipelineState() {
      return state;
    }

    public Instant getCreationTimestamp() {
      return creationTimestamp;
    }

    public HddsProtos.ReplicationType getType() {
      return replicationConfig.getReplicationType();
    }

    public boolean isEmpty() {
      return nodeStatus.isEmpty();
    }

    public List<DatanodeDetails> getNodes() {
      return new ArrayList<>(nodeStatus.keySet());
    }

    public boolean isAllocationTimeout() {
      return false;
    }

    public boolean isHealthy() {
      return false; // leaderId is always null, So pipeline is unhealthy
    }
  }
}
