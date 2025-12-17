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

package org.apache.hadoop.ozone.recon.api;

import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.recon.api.types.DataNodeMetricsServiceResponse;
import org.apache.hadoop.ozone.recon.api.types.ScmPendingDeletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST API endpoint that provides metrics and information related to
 * pending deletions. It responds to requests on the "/pendingDeletion" path
 * and produces application/json responses.
 */
@Path("/pendingDeletion")
@Produces("application/json")
@AdminOnly
public class PendingDeletionEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(PendingDeletionEndpoint.class);
  private final ReconGlobalMetricsService reconGlobalMetricsService;
  private final DataNodeMetricsService dataNodeMetricsService;
  private final StorageContainerLocationProtocol scmClient;

  @Inject
  public PendingDeletionEndpoint(
      ReconGlobalMetricsService reconGlobalMetricsService,
      DataNodeMetricsService dataNodeMetricsService,
      StorageContainerLocationProtocol scmClient) {
    this.reconGlobalMetricsService = reconGlobalMetricsService;
    this.dataNodeMetricsService = dataNodeMetricsService;
    this.scmClient = scmClient;
  }

  @GET
  public Response getPendingDeletionByComponent(@QueryParam("component") String component) {
    if (component == null || component.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("component query parameter is required").build();
    }
    final String normalizedComponent = component.trim().toLowerCase();
    switch (normalizedComponent) {
    case "dn":
      return handleDataNodeMetrics();
    case "scm":
      return handleScmPendingDeletion();
    case "om":
      return handleOmPendingDeletion();
    default:
      return Response.status(Response.Status.BAD_REQUEST)
          .entity("component query parameter must be one of dn, scm, om").build();
    }
  }

  private Response handleDataNodeMetrics() {
    DataNodeMetricsServiceResponse response = dataNodeMetricsService.getCollectedMetrics();
    if (response.getStatus() == DataNodeMetricsService.MetricCollectionStatus.FINISHED) {
      return Response.ok(response).build();
    } else {
      return Response.accepted(response).build();
    }
  }

  private Response handleScmPendingDeletion() {
    try {
      HddsProtos.DeletedBlocksTransactionSummary summary = scmClient.getDeletedBlockSummary();
      if (summary == null) {
        return Response.noContent()
            .build();
      }
      ScmPendingDeletion pendingDeletion = new ScmPendingDeletion(
          summary.getTotalBlockSize(),
          summary.getTotalBlockReplicatedSize(),
          summary.getTotalBlockCount());
      return Response.ok(pendingDeletion).build();
    } catch (Exception e) {
      LOG.error("Failed to get pending deletion info from SCM", e);
      ScmPendingDeletion pendingDeletion = new ScmPendingDeletion(-1L, -1L, -1L);
      return Response.ok(pendingDeletion).build();
    }
  }

  private Response handleOmPendingDeletion() {
    Map<String, Long> pendingDeletion = reconGlobalMetricsService.calculatePendingSizes();
    return Response.ok(pendingDeletion).build();
  }
}
