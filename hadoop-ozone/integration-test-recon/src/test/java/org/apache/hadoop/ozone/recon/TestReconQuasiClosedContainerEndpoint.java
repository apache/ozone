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

package org.apache.hadoop.ozone.recon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.recon.api.ContainerEndpoint;
import org.apache.hadoop.ozone.recon.api.types.QuasiClosedContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.QuasiClosedContainersResponse;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests for the GET /containers/quasiClosed endpoint.
 *
 * The cluster is started once for the entire test class (@TestInstance.PER_CLASS)
 * so the expensive MiniOzoneCluster boot only happens once instead of once per test.
 *
 * Each test allocates containers using unique IDs from CONTAINER_ID_SEQ and uses
 * those IDs as pagination cursors so tests don't interfere with each other.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestReconQuasiClosedContainerEndpoint {

  private static final int PIPELINE_READY_TIMEOUT_MS = 30000;
  private static final int POLL_INTERVAL_MS = 500;

  /**
   * Monotonically increasing ID counter. Each test records its start ID and
   * uses (startId - 1) as the minContainerId cursor so it only sees its own
   * containers when paginating.
   */
  private final AtomicLong containerIdSeq = new AtomicLong(10000L);

  private MiniOzoneCluster cluster; // NOPMD - shared across @BeforeAll and @AfterAll
  private ReconService recon; // NOPMD
  private ContainerEndpoint containerEndpoint;
  private ReconContainerManager reconContainerManager;
  private ReconStorageContainerManagerFacade reconScm;

  @BeforeAll
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    recon = new ReconService(conf);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .addService(recon)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.THREE, 30000);

    reconScm = (ReconStorageContainerManagerFacade)
        recon.getReconServer().getReconStorageContainerManager();

    // Wait for Recon's pipeline manager to be populated from SCM.
    LambdaTestUtils.await(PIPELINE_READY_TIMEOUT_MS, POLL_INTERVAL_MS,
        () -> !reconScm.getPipelineManager().getPipelines(
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
            .isEmpty());

    reconContainerManager = (ReconContainerManager) reconScm.getContainerManager();

    containerEndpoint = new ContainerEndpoint(
        reconScm,
        null,  // ContainerHealthSchemaManager — not needed
        null,  // ReconNamespaceSummaryManager — not needed
        null,  // ReconContainerMetadataManager — not needed
        null,  // ReconOMMetadataManager — not needed
        null); // ExportJobManager — not needed
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(cluster);
  }

  /**
   * Injects a container with the next available ID directly into Recon's
   * in-memory state — no RPC sync needed. Returns the assigned ID.
   */
  private long createQuasiClosedContainer() throws Exception {
    long id = containerIdSeq.getAndIncrement();
    Pipeline pipeline = reconScm.getPipelineManager()
        .getPipelines(
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .get(0);

    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(id)
        .setNumberOfKeys(5)
        .setPipelineID(pipeline.getId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setOwner("test")
        .setState(HddsProtos.LifeCycleState.OPEN)
        .build();

    reconContainerManager.addNewContainer(
        new ContainerWithPipeline(containerInfo, pipeline));
    reconContainerManager.updateContainerState(
        ContainerID.valueOf(id), HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(
        ContainerID.valueOf(id), HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    return id;
  }

  @Test
  public void testBasicQuasiClosedList() throws Exception {
    long startId = containerIdSeq.get();
    long id1 = createQuasiClosedContainer();
    long id2 = createQuasiClosedContainer();

    // Use (startId - 1) so we only see containers created in this test.
    Response response = containerEndpoint.getQuasiClosedContainers(1000, startId - 1);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    QuasiClosedContainersResponse result =
        (QuasiClosedContainersResponse) response.getEntity();
    assertNotNull(result);

    List<Long> returnedIds = result.getContainers().stream()
        .map(QuasiClosedContainerMetadata::getContainerID)
        .collect(Collectors.toList());
    assertTrue(returnedIds.contains(id1));
    assertTrue(returnedIds.contains(id2));

    result.getContainers().forEach(c -> {
      assertEquals(3L, c.getExpectedReplicaCount());
      assertTrue(c.getStateEnterTime() >= 0);
      assertNotNull(c.getPipelineID());
    });
  }

  @Test
  public void testPagination() throws Exception {
    final int totalContainers = 25;
    final int pageSize = 7;

    long startId = containerIdSeq.get();
    for (int i = 0; i < totalContainers; i++) {
      createQuasiClosedContainer();
    }
    long endId = containerIdSeq.get() - 1;

    // Walk pages using the cursor, collecting only IDs in our range [startId, endId].
    List<Long> allReturnedIds = new ArrayList<>();
    long cursor = startId - 1;
    int pagesVisited = 0;

    while (true) {
      QuasiClosedContainersResponse page =
          (QuasiClosedContainersResponse)
              containerEndpoint.getQuasiClosedContainers(pageSize, cursor).getEntity();

      List<Long> pageIds = page.getContainers().stream()
          .map(QuasiClosedContainerMetadata::getContainerID)
          .filter(id -> id >= startId && id <= endId)
          .collect(Collectors.toList());

      if (pageIds.isEmpty()) {
        break;
      }

      // No ID from this page should have been seen before.
      for (Long id : pageIds) {
        assertTrue(!allReturnedIds.contains(id),
            "Duplicate container ID across pages: " + id);
      }

      allReturnedIds.addAll(pageIds);
      cursor = page.getLastKey();
      pagesVisited++;
    }

    assertEquals(totalContainers, allReturnedIds.size(),
        "All created containers must be returned across pages");
    // 25 containers / pageSize 7 = ceil(25/7) = 4 pages
    assertEquals(4, pagesVisited);
  }

  @Test
  public void testLimitZeroReturnsCountOnly() throws Exception {
    createQuasiClosedContainer();
    createQuasiClosedContainer();

    // limit=0 must return empty containers but a non-zero total count.
    QuasiClosedContainersResponse result =
        (QuasiClosedContainersResponse)
            containerEndpoint.getQuasiClosedContainers(0, 0L).getEntity();

    assertTrue(result.getContainers() == null || result.getContainers().isEmpty(),
        "limit=0 must return empty container list");
    assertTrue(result.getQuasiClosedCount() >= 2,
        "quasiClosedCount must reflect all quasi-closed containers");
  }

  @Test
  public void testInvalidInputsReturnBadRequest() {
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        containerEndpoint.getQuasiClosedContainers(10, -1L).getStatus(),
        "Negative minContainerId must return 400");
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(),
        containerEndpoint.getQuasiClosedContainers(-1, 0L).getStatus(),
        "Negative limit must return 400");
  }
}
