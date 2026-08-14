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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.recon.metrics.ReconScmContainerSyncMetrics;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestReconStorageContainerSyncHelper {

  private final StorageContainerServiceProvider mockScmServiceProvider =
      mock(StorageContainerServiceProvider.class);

  private final ReconContainerManager mockContainerManager =
      mock(ReconContainerManager.class);

  private ReconScmContainerSyncMetrics metrics;
  private ReconStorageContainerSyncHelper syncHelper;

  @BeforeEach
  void setUp() {
    metrics = ReconScmContainerSyncMetrics.create();
    syncHelper = new ReconStorageContainerSyncHelper(
        mockScmServiceProvider,
        new OzoneConfiguration(),
        mockContainerManager,
        metrics
    );
  }

  @AfterEach
  void tearDown() {
    metrics.unRegister();
  }

  @Test
  void testContainerSyncMetricsTrackPreSyncDriftAndDuration() throws Exception {
    when(mockScmServiceProvider.getContainerCount(OPEN)).thenReturn(5L);
    when(mockScmServiceProvider.getContainerCount(QUASI_CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(8L);
    when(mockScmServiceProvider.getContainerCount(DELETED)).thenReturn(9L);
    when(mockContainerManager.getContainerStateCount(OPEN)).thenReturn(3);
    when(mockContainerManager.getContainerStateCount(QUASI_CLOSED)).thenReturn(1);
    when(mockContainerManager.getContainerStateCount(CLOSED)).thenReturn(10);
    when(mockContainerManager.getContainerStateCount(DELETED)).thenReturn(7);
    when(mockScmServiceProvider.getListOfContainerIDs(
        any(), any(Integer.class), any())).thenReturn(Collections.emptyList());

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    assertEquals(2L, metrics.getContainerCountDrift(OPEN));
    assertEquals(0L, metrics.getContainerCountDrift(QUASI_CLOSED));
    assertEquals(-2L, metrics.getContainerCountDrift(CLOSED));
    assertEquals(2L, metrics.getContainerCountDrift(DELETED));
    assertTrue(metrics.getContainerSyncDurationMs(OPEN) >= 0);
    assertTrue(metrics.getContainerSyncDurationMs(QUASI_CLOSED) >= 0);
    assertTrue(metrics.getContainerSyncDurationMs(CLOSED) >= 0);
    assertTrue(metrics.getContainerSyncDurationMs(DELETED) >= 0);
  }

  @Test
  void testContainerMissingFromReconIsAdded() throws Exception {
    ContainerID cid = ContainerID.valueOf(42L);
    ContainerInfo info = new ContainerInfo.Builder()
        .setContainerID(42L)
        .setState(CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test")
        .build();
    ContainerWithPipeline cwp = new ContainerWithPipeline(info, null);

    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
        .thenReturn(Collections.singletonList(cid));
    when(mockContainerManager.containerExist(cid)).thenReturn(false);
    // Pass 1 now uses getExistContainerWithPipelinesInBatch for missing containers so that
    // the null-pipeline fallback prevents silent skipping when pipeline lookups fail.
    when(mockScmServiceProvider.getExistContainerWithPipelinesInBatch(
        Collections.singletonList(42L))).thenReturn(Collections.singletonList(cwp));

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    verify(mockScmServiceProvider).getExistContainerWithPipelinesInBatch(
        Collections.singletonList(42L));
    verify(mockContainerManager).addNewContainer(cwp);
  }

  @Test
  void testContainerMissingFromReconIsAddedWhenMultiplePages() throws Exception {
    // Force containerCountPerCall = 2 via the batch size config
    OzoneConfiguration pagedConf = new OzoneConfiguration();
    pagedConf.setLong(OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE, 2L);
    ReconStorageContainerSyncHelper pagedHelper = new ReconStorageContainerSyncHelper(
        mockScmServiceProvider, pagedConf, mockContainerManager, metrics);

    // Page 1: containers 1 and 2 (both missing from Recon)
    ContainerID cid1 = ContainerID.valueOf(1L);
    ContainerID cid2 = ContainerID.valueOf(2L);
    ContainerInfo info1 = new ContainerInfo.Builder()
        .setContainerID(1L).setState(CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test").build();
    ContainerInfo info2 = new ContainerInfo.Builder()
        .setContainerID(2L).setState(CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test").build();
    ContainerWithPipeline cwp1 = new ContainerWithPipeline(info1, null);
    ContainerWithPipeline cwp2 = new ContainerWithPipeline(info2, null);

    // Page 2: container 3 (already in Recon)
    ContainerID cid3 = ContainerID.valueOf(3L);

    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(3L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(2), eq(CLOSED)))
        .thenReturn(Arrays.asList(cid1, cid2));
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(3L)), eq(2), eq(CLOSED)))
        .thenReturn(Collections.singletonList(cid3));

    // Stub getContainer for cid3 (exists in Recon) so processSyncedClosedContainer
    // reads its state and confirms no correction is needed.
    ContainerInfo closedInfo3 = new ContainerInfo.Builder()
        .setContainerID(3L)
        .setState(CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test")
        .build();

    when(mockContainerManager.containerExist(cid1)).thenReturn(false);
    when(mockContainerManager.containerExist(cid2)).thenReturn(false);
    when(mockContainerManager.containerExist(cid3)).thenReturn(true);
    when(mockContainerManager.getContainer(cid3)).thenReturn(closedInfo3);
    // Pass 1 collects all absent IDs on a page and fetches them in ONE batch call.
    // Page 1 has cid1 and cid2 both absent → single call with [1L, 2L].
    when(mockScmServiceProvider.getExistContainerWithPipelinesInBatch(
        Arrays.asList(1L, 2L)))
        .thenReturn(Arrays.asList(cwp1, cwp2));
    // Page 2: cid3 already exists in Recon; no batch call needed for that page.

    boolean result = pagedHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    // Page 1: both missing containers were added
    verify(mockContainerManager).addNewContainer(cwp1);
    verify(mockContainerManager).addNewContainer(cwp2);
    // Page 2: present container was skipped
    verify(mockContainerManager, never()).addNewContainer(
        argThat(cwp -> cwp.getContainerInfo().getContainerID() == 3L));
    // Both pages were fetched
    verify(mockScmServiceProvider).getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(2), eq(CLOSED));
    verify(mockScmServiceProvider).getListOfContainerIDs(
        eq(ContainerID.valueOf(3L)), eq(2), eq(CLOSED));
  }

  @Test
  void testContainerAlreadyInReconIsSkipped() throws Exception {
    ContainerID cid = ContainerID.valueOf(7L);
    // Stub getContainer to return a CLOSED container so processSyncedClosedContainer
    // finds no state drift and returns without further action.
    ContainerInfo closedInfo = new ContainerInfo.Builder()
        .setContainerID(7L)
        .setState(CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setOwner("test")
        .build();

    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
        .thenReturn(Collections.singletonList(cid));
    when(mockContainerManager.containerExist(cid)).thenReturn(true);
    when(mockContainerManager.getContainer(cid)).thenReturn(closedInfo);

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    // Container already in Recon: no batch fetch needed, no add attempted.
    verify(mockScmServiceProvider, never()).getExistContainerWithPipelinesInBatch(any());
    verify(mockContainerManager, never()).addNewContainer(any());
  }

  @Test
  void testZeroClosedContainersReturnsTrue() throws Exception {
    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(0L);

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    // Other state passes may still run, so assert that the CLOSED pass was
    // skipped and no meaningful mutations happened.
    verify(mockContainerManager, never()).addNewContainer(any());
    verify(mockContainerManager, never()).updateContainerState(any(), any());
    verify(mockScmServiceProvider, never())
        .getListOfContainerIDs(any(), any(Integer.class), eq(CLOSED));
  }

  @Test
  void testEmptyListFromSCMReturnsTrue() throws Exception {
    // SCM reports 1 CLOSED container but getListOfContainerIDs returns empty —
    // this can happen when the count RPC and list RPC are not atomic (e.g., the
    // container was deleted between the two calls). Pass 1 treats an empty page
    // as "end of pagination" and returns true; it does NOT treat this as an error.
    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
        .thenReturn(Collections.emptyList());

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    verify(mockContainerManager, never()).addNewContainer(any());
    verify(mockContainerManager, never()).updateContainerState(any(), any());
  }

}
