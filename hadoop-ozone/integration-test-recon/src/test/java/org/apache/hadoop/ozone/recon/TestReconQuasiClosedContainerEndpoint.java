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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the GET /containers/quasiClosed endpoint.
 *
 * Containers are injected directly into Recon's in-memory container manager
 * using a pipeline that Recon already knows about (populated from SCM during
 * cluster startup). This avoids RPC sync round-trips and is deterministic.
 */
public class TestReconQuasiClosedContainerEndpoint {

  private static final int PIPELINE_READY_TIMEOUT_MS = 30000;
  private static final int POLL_INTERVAL_MS = 500;

  /** Monotonically increasing container ID counter so tests don't collide. */
  private static final AtomicLong CONTAINER_ID_SEQ = new AtomicLong(10000L);

  private MiniOzoneCluster cluster;
  private ReconService recon;
  private ContainerEndpoint containerEndpoint;
  private ReconContainerManager reconContainerManager;
  private ReconStorageContainerManagerFacade reconScm;

  @BeforeEach
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
        null,  // ContainerHealthSchemaManager — not needed for quasi-closed tests
        null,  // ReconNamespaceSummaryManager — not needed
        null,  // ReconContainerMetadataManager — not needed
        null,  // ReconOMMetadataManager — not needed
        null); // ExportJobManager — not needed
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(cluster);
  }

  /**
   * Creates a new container in Recon and drives it to QUASI_CLOSED via
   * OPEN -> FINALIZE (CLOSING) -> QUASI_CLOSE (QUASI_CLOSED).
   * Uses a pipeline that Recon already knows about — no RPC sync needed.
   */
  private ContainerInfo createQuasiClosedContainer() throws Exception {
    long id = CONTAINER_ID_SEQ.getAndIncrement();
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

    return reconContainerManager.getContainer(ContainerID.valueOf(id));
  }

  @Test
  public void testEmptyWhenNoQuasiClosedContainers() {
    Response response = containerEndpoint.getQuasiClosedContainers(1000, 0L);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    QuasiClosedContainersResponse result =
        (QuasiClosedContainersResponse) response.getEntity();
    assertNotNull(result);
    assertTrue(result.getContainers() == null || result.getContainers().isEmpty());
    assertEquals(0, result.getQuasiClosedCount());
  }

  @Test
  public void testBasicQuasiClosedList() throws Exception {
    ContainerInfo c1 = createQuasiClosedContainer();
    ContainerInfo c2 = createQuasiClosedContainer();

    Response response = containerEndpoint.getQuasiClosedContainers(1000, 0L);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    QuasiClosedContainersResponse result =
        (QuasiClosedContainersResponse) response.getEntity();
    assertNotNull(result);
    assertEquals(2, result.getQuasiClosedCount());
    assertEquals(2, result.getContainers().size());

    List<Long> returnedIds = result.getContainers().stream()
        .map(QuasiClosedContainerMetadata::getContainerID)
        .collect(Collectors.toList());
    assertTrue(returnedIds.contains(c1.getContainerID()));
    assertTrue(returnedIds.contains(c2.getContainerID()));

    result.getContainers().forEach(c -> {
      // RATIS THREE → requiredNodes = 3
      assertEquals(3L, c.getExpectedReplicaCount());
      assertTrue(c.getStateEnterTime() >= 0);
      assertNotNull(c.getPipelineID());
    });
  }

  @Test
  public void testPagination() throws Exception {
    final int totalContainers = 25;
    final int pageSize = 7;

    for (int i = 0; i < totalContainers; i++) {
      createQuasiClosedContainer();
    }

    // Walk all pages using the cursor and collect every returned ID.
    List<Long> allReturnedIds = new java.util.ArrayList<>();
    long cursor = 0L;
    int pagesVisited = 0;

    while (true) {
      QuasiClosedContainersResponse page =
          (QuasiClosedContainersResponse)
              containerEndpoint.getQuasiClosedContainers(pageSize, cursor).getEntity();

      // Total count must be consistent on every page.
      assertEquals(totalContainers, page.getQuasiClosedCount(),
          "quasiClosedCount must be stable across pages");

      List<Long> pageIds = page.getContainers().stream()
          .map(QuasiClosedContainerMetadata::getContainerID)
          .collect(Collectors.toList());

      if (pageIds.isEmpty()) {
        break;
      }

      // No ID on this page should have been seen on a previous page.
      for (Long id : pageIds) {
        assertTrue(!allReturnedIds.contains(id),
            "Duplicate container ID across pages: " + id);
      }

      allReturnedIds.addAll(pageIds);
      cursor = page.getLastKey();
      pagesVisited++;

      // Each page except possibly the last must be full.
      if (allReturnedIds.size() < totalContainers) {
        assertEquals(pageSize, pageIds.size(),
            "Non-final page must return exactly pageSize containers");
      }
    }

    // All containers must have been returned exactly once across all pages.
    assertEquals(totalContainers, allReturnedIds.size(),
        "Total containers across all pages must equal totalContainers");

    // 25 containers with pageSize 7 → 3 full pages (7+7+7=21) + 1 partial (4) = 4 pages.
    assertEquals(4, pagesVisited, "Expected 4 pages for 25 containers with pageSize 7");
  }

  @Test
  public void testLimitZeroReturnsCountOnly() throws Exception {
    createQuasiClosedContainer();
    createQuasiClosedContainer();

    QuasiClosedContainersResponse result =
        (QuasiClosedContainersResponse)
            containerEndpoint.getQuasiClosedContainers(0, 0L).getEntity();

    assertTrue(result.getContainers() == null || result.getContainers().isEmpty(),
        "limit=0 must return empty container list");
    assertEquals(2, result.getQuasiClosedCount(),
        "limit=0 must still return correct total count");
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
