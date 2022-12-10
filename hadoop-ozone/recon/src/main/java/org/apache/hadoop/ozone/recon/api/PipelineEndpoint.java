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
import org.apache.hadoop.ozone.recon.MetricsServiceProviderFactory;
import org.apache.hadoop.ozone.recon.api.types.PipelineMetadata;
import org.apache.hadoop.ozone.recon.api.types.PipelinesResponse;
import org.apache.hadoop.ozone.recon.metrics.Metric;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
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
import java.util.TreeMap;
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
  private MetricsServiceProvider metricsServiceProvider;

  @Inject
  PipelineEndpoint(OzoneStorageContainerManager reconSCM,
                   MetricsServiceProviderFactory
                       metricsServiceProviderFactory) {
    this.pipelineManager = (ReconPipelineManager) reconSCM.getPipelineManager();
    this.metricsServiceProvider =
        metricsServiceProviderFactory.getMetricsServiceProvider();
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

      PipelineMetadata.Builder pipelineBuilder =
          builder.setPipelineId(pipelineId)
              .setDatanodes(datanodes)
              .setDuration(duration)
              .setStatus(pipeline.getPipelineState())
              .setReplicationConfig(pipeline.getReplicationConfig());
      // If any metrics service providers like Prometheus
      // is configured, then query it for metrics and populate
      // leader election count and last leader election time
      if (metricsServiceProvider != null) {
        // Extract last part of pipelineId to get its group Id.
        // ex. group id of 48981bf7-8bea-4fbd-9857-79df51ee872d
        // is group-79DF51EE872D
        String[] splits = pipelineId.toString().split("-");
        String groupId = "group-" + splits[splits.length - 1].toUpperCase();
        Long leaderElectionCount = getElectionCountMetricValue(groupId);
        pipelineBuilder.setLeaderElections(leaderElectionCount);
        Long lastLeaderElectionElapsedTime
                = getLastLeaderElectionElapsedTimeMetricValue(groupId,
                pipeline.getLeaderId());
        pipelineBuilder.setLastLeaderElection(lastLeaderElectionElapsedTime);
      }

      pipelinesList.add(pipelineBuilder.build());
    });

    PipelinesResponse pipelinesResponse =
        new PipelinesResponse(pipelinesList.size(), pipelinesList);
    return Response.ok(pipelinesResponse).build();
  }

  private Long getElectionCountMetricValue(String groupId) {
    Long electionCount = 0L;
    String metricsQuery = String.format(
            "query=ratis_leader_election_electionCount{group=\"%s\"}", groupId);
    try {
      List<Metric> metrics = metricsServiceProvider.getMetricsInstant(
              metricsQuery);
      if (!metrics.isEmpty()) {
        for (Metric m : metrics) {
          TreeMap<Double, Double> values = (TreeMap<Double, Double>)
                  m.getValues();
          if (!values.isEmpty()) {
            electionCount += values.firstEntry().getValue().longValue();
          }
        }
      }
    } catch (Exception ex) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Unable to get metrics value for " +
                "ratis_leader_election_electionCount", ex);
      }
    }
    return electionCount;
  }

  private Long getLastLeaderElectionElapsedTimeMetricValue(String groupId,
                                                           UUID uuid) {
    String metricsQuery = String.format(
            "query=ratis_leader_election_lastLeaderElectionElapsedTime{group=" +
                    "\"%s\",exported_instance=\"%s\"}", groupId,
            uuid.toString());
    try {
      List<Metric> metrics = metricsServiceProvider.getMetricsInstant(
              metricsQuery);
      if (!metrics.isEmpty()) {
        TreeMap<Double, Double> values = (TreeMap<Double, Double>)
                metrics.get(0).getValues();
        if (!values.isEmpty()) {
          return values.firstEntry().getValue().longValue();
        }
      }
    } catch (Exception ex) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Unable to get metrics value for " +
                "ratis_leader_election_lastLeaderElectionElapsedTime", ex);
      }
    }
    return 0L;
  }
}
