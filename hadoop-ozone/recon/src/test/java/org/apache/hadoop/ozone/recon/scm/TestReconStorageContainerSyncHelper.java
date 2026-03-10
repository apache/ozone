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
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.Test;

class TestReconStorageContainerSyncHelper {

  private final StorageContainerServiceProvider mockScmServiceProvider =
      mock(StorageContainerServiceProvider.class);

  private final ReconContainerManager mockContainerManager =
      mock(ReconContainerManager.class);

  private final ReconStorageContainerSyncHelper syncHelper;

  TestReconStorageContainerSyncHelper() {
    syncHelper = new ReconStorageContainerSyncHelper(
        mockScmServiceProvider,
        new OzoneConfiguration(),
        mockContainerManager
    );
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
    when(mockScmServiceProvider.getContainerWithPipeline(42L)).thenReturn(cwp);

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    verify(mockScmServiceProvider).getContainerWithPipeline(42L);
    verify(mockContainerManager).addNewContainer(cwp);
  }

  @Test
  void testContainerMissingFromReconIsAddedWhenMultiplePages() throws Exception {
    // Force containerCountPerCall = 2 via the batch size config
    OzoneConfiguration pagedConf = new OzoneConfiguration();
    pagedConf.setLong(OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE, 2L);
    ReconStorageContainerSyncHelper pagedHelper = new ReconStorageContainerSyncHelper(
        mockScmServiceProvider, pagedConf, mockContainerManager);

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

    when(mockContainerManager.containerExist(cid1)).thenReturn(false);
    when(mockContainerManager.containerExist(cid2)).thenReturn(false);
    when(mockContainerManager.containerExist(cid3)).thenReturn(true);
    when(mockScmServiceProvider.getContainerWithPipeline(1L)).thenReturn(cwp1);
    when(mockScmServiceProvider.getContainerWithPipeline(2L)).thenReturn(cwp2);

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

    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
        .thenReturn(Collections.singletonList(cid));
    when(mockContainerManager.containerExist(cid)).thenReturn(true);

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    verify(mockScmServiceProvider, never()).getContainerWithPipeline(anyLong());
    verify(mockContainerManager, never()).addNewContainer(any());
  }

  @Test
  void testZeroClosedContainersReturnsTrue() throws Exception {
    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(0L);

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertTrue(result);
    verifyNoInteractions(mockContainerManager);
    verify(mockScmServiceProvider, never())
        .getListOfContainerIDs(any(), any(Integer.class), any());
  }

  @Test
  void testEmptyListFromSCMReturnsFalse() throws Exception {
    when(mockScmServiceProvider.getContainerCount(CLOSED)).thenReturn(1L);
    when(mockScmServiceProvider.getListOfContainerIDs(
        eq(ContainerID.valueOf(1L)), eq(1), eq(CLOSED)))
        .thenReturn(Collections.emptyList());

    boolean result = syncHelper.syncWithSCMContainerInfo();

    assertFalse(result);
    verifyNoInteractions(mockContainerManager);
  }

}
