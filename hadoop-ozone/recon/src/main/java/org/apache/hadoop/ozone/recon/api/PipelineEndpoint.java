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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Endpoint to fetch details about Pipelines.
 */
@Path("/pipelines")
@Produces(MediaType.APPLICATION_JSON)
public class PipelineEndpoint {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineEndpoint.class);

  private ReconPipelineManager pipelineManager;

  @Inject
  PipelineEndpoint(OzoneStorageContainerManager reconSCM) {
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
  }

  /**
   * Return the list of pipelines with detailed information about each pipeline.
   * @return {@link Response}
   */
  @GET
  public Response getPipelines() {
    List<PipelineMetadata> pipelinesList = new ArrayList<>();
    List<Pipeline> pipelines = pipelineManager.getPipelines();

    pipelines.forEach(pipeline -> {
      UUID pipelineId = pipeline.getId().getId();
      List<String> datanodes = new ArrayList<>();
      PipelineMetadata.Builder builder = PipelineMetadata.newBuilder();
      pipeline.getNodes().forEach(node -> datanodes.add(node.getHostName()));
      long duration =
          Instant.now().toEpochMilli() -
              pipeline.getCreationTimestamp().toEpochMilli();

      try {
        String leaderNode = pipeline.getLeaderNode().getHostName();
        builder.setLeaderNode(leaderNode);
      } catch (IOException ioEx) {
        LOG.warn("Cannot get leader node for pipeline {}",
            pipelineId, ioEx);
      }

      try {
        int containers =
            pipelineManager.getNumberOfContainers(pipeline.getId());
        builder.setContainers(containers);
      } catch (IOException ioEx) {
        LOG.warn("Cannot get containers for pipeline {} ", pipelineId, ioEx);
      }

      PipelineMetadata pipelineMetadata = builder.setPipelineId(pipelineId)
          .setDatanodes(datanodes)
          .setDuration(duration)
          .setStatus(pipeline.getPipelineState())
          .setReplicationFactor(pipeline.getReplication())
          .setReplicationType(pipeline.getType().toString())
          .build();

      pipelinesList.add(pipelineMetadata);
    });

    PipelinesResponse pipelinesResponse =
        new PipelinesResponse(pipelinesList.size(), pipelinesList);
    return Response.ok(pipelinesResponse).build();
  }
}
