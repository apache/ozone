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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
