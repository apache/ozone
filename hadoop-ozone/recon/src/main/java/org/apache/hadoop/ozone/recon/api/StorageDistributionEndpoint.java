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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.api.types.CommittedBytes;
import org.apache.hadoop.ozone.recon.api.types.DatanodeStorageReport;
import org.apache.hadoop.ozone.recon.api.types.DeletionPendingBytesByStage;
import org.apache.hadoop.ozone.recon.api.types.GlobalNamespaceReport;
import org.apache.hadoop.ozone.recon.api.types.GlobalStorageReport;
import org.apache.hadoop.ozone.recon.api.types.StorageCapacityDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.UsedSpaceBreakDown;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;

/**
 * This endpoint handles requests related to storage distribution across
 * different datanodes in a Recon instance. It provides detailed reports
 * on storage capacity, utilization, and associated metrics.
 *
 * The data is aggregated from multiple sources, including node manager
 * statistics, and is used to construct responses with information
 * about global storage and namespace usage, storage usage breakdown,
 * and deletion operations in progress.
 *
 * An instance of {@link ReconNodeManager} is used to fetch detailed
 * node-specific statistics required for generating the report.
 */
@Path("/storagedistribution")
@Produces("application/json")
public class StorageDistributionEndpoint {
  private final ReconNodeManager nodeManager;

  @Inject
  public StorageDistributionEndpoint(OzoneStorageContainerManager reconSCM) {
    this.nodeManager = (ReconNodeManager) reconSCM.getScmNodeManager();
  }

  @GET
  public Response getStorageDistribution() {
    List<DatanodeStorageReport> nodeStorageReports = new ArrayList<>();
    StorageCapacityDistributionResponse.Builder responseBuilder =  StorageCapacityDistributionResponse.newBuilder();
    nodeManager.getAllNodes().forEach(dn -> {
      nodeStorageReports.add(getStorageReport(dn));
    });

    Map<String, Map<String, Long>> stageItems  = new HashMap<>();
    Map<String, Long> omStage = new HashMap<>();
    omStage.put("pendingBytes", 0L);
    stageItems.put("OM", omStage);

    Map<String, Long> scmStage = new HashMap<>();
    scmStage.put("pendingBytes", 0L);
    stageItems.put("SCM", scmStage);

    Map<String, Long> dnStage = new HashMap<>();
    dnStage.put("pendingBytes", 0L);
    stageItems.put("DN", dnStage);
    DeletionPendingBytesByStage deletionPendingBytesByStage =  new DeletionPendingBytesByStage(0, stageItems);

    StorageCapacityDistributionResponse response = responseBuilder
        .setDataNodeUsage(nodeStorageReports)
        .setGlobalStorage(new GlobalStorageReport(0, 0, 0))
        .setGlobalNamespace(new GlobalNamespaceReport(0, 0))
        .setUsedSpaceBreakDown(new UsedSpaceBreakDown(0,
            new CommittedBytes(0, 0, 0, 0),
            deletionPendingBytesByStage))
        .build();
    return Response.ok(response).build();
  }

  private DatanodeStorageReport getStorageReport(DatanodeDetails datanode) {
    SCMNodeStat nodeStat =
        nodeManager.getNodeStat(datanode).get();
    long capacity = nodeStat.getCapacity().get();
    long used = nodeStat.getScmUsed().get();
    long remaining = nodeStat.getRemaining().get();
    long committed = nodeStat.getCommitted().get();
    long pendingDeletions = 0; // TODO nodeStat.getPendingDeletions().get();
    return new DatanodeStorageReport(capacity, used, remaining, committed, pendingDeletions);
  }
}
