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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .ContainerWithPipeline;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * This is the handler that process container info command.
 */
@Command(
    name = "info",
    description = "Show information about a specific container",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class InfoSubcommand extends ScmSubcommand {

  private static final Logger LOG =
      LoggerFactory.getLogger(InfoSubcommand.class);

  @Spec
  private CommandSpec spec;

  @CommandLine.Option(names = { "--json" },
      defaultValue = "false",
      description = "Format output as JSON")
  private boolean json;

  @Parameters(description = "Decimal id of the container.")
  private long containerID;

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    final ContainerWithPipeline container = scmClient.
        getContainerWithPipeline(containerID);
    Preconditions.checkNotNull(container, "Container cannot be null");
    List<ContainerReplicaInfo> replicas = null;
    try {
      replicas = scmClient.getContainerReplicas(containerID);
    } catch (IOException e) {
      LOG.error("Unable to retrieve the replica details", e);
    }

    if (json) {
      ContainerWithPipelineAndReplicas wrapper =
          new ContainerWithPipelineAndReplicas(container.getContainerInfo(),
              container.getPipeline(), replicas);
      LOG.info(JsonUtils.toJsonStringWithDefaultPrettyPrinter(wrapper));
    } else {
      // Print container report info.
      LOG.info("Container id: {}", containerID);
      boolean verbose = spec != null
          && spec.root().userObject() instanceof GenericParentCommand
          && ((GenericParentCommand) spec.root().userObject()).isVerbose();
      if (verbose) {
        LOG.info("Pipeline Info: {}", container.getPipeline());
      } else {
        LOG.info("Pipeline id: {}", container.getPipeline().getId().getId());
      }
      LOG.info("Container State: {}", container.getContainerInfo().getState());

      // Print pipeline of an existing container.
      String machinesStr = container.getPipeline().getNodes().stream().map(
          InfoSubcommand::buildDatanodeDetails)
          .collect(Collectors.joining(",\n"));
      LOG.info("Datanodes: [{}]", machinesStr);

      // Print the replica details if available
      if (replicas != null) {
        String replicaStr = replicas.stream()
            .sorted(Comparator.comparing(ContainerReplicaInfo::getReplicaIndex))
            .map(InfoSubcommand::buildReplicaDetails)
            .collect(Collectors.joining(",\n"));
        LOG.info("Replicas: [{}]", replicaStr);
      }
    }
  }

  private static String buildDatanodeDetails(DatanodeDetails details) {
    return details.getUuidString() + "/" + details.getHostName();
  }

  private static String buildReplicaDetails(ContainerReplicaInfo replica) {
    StringBuilder sb = new StringBuilder();
    sb.append("State: " + replica.getState() + ";");
    if (replica.getReplicaIndex() != -1) {
      sb.append(" ReplicaIndex: " + replica.getReplicaIndex() + ";");
    }
    sb.append(" Origin: " + replica.getPlaceOfBirth().toString() + ";");
    sb.append(" Location: "
        + buildDatanodeDetails(replica.getDatanodeDetails()));
    return sb.toString();
  }

  private static class ContainerWithPipelineAndReplicas {

    private ContainerInfo containerInfo;
    private Pipeline pipeline;
    private List<ContainerReplicaInfo> replicas;

    ContainerWithPipelineAndReplicas(ContainerInfo container, Pipeline pipeline,
                                     List<ContainerReplicaInfo> replicas) {
      this.containerInfo = container;
      this.pipeline = pipeline;
      this.replicas = replicas;
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
  }
}
