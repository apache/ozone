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

package org.apache.hadoop.hdds.scm.block;

import static org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator.LOCAL_ID;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.WritableContainerFactory;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Unit tests for BlockManagerImpl storage-type fallback logic.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestBlockManagerImpl {

  private static final long DEFAULT_BLOCK_SIZE = 128 * MB;
  private static final long CONTAINER_SIZE = DEFAULT_BLOCK_SIZE * 4;
  private static final ReplicationConfig REPLICATION_CONFIG =
      RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final String OWNER = "testOwner";

  @Mock
  private WritableContainerFactory mockFactory;
  @Mock
  private PipelineManager mockPipelineManager;
  @Mock
  private SequenceIdGenerator mockSequenceIdGen;
  @Mock
  private StorageContainerManager mockScm;
  @Mock
  private SCMContext mockScmContext;
  @Mock
  private ContainerInfo mockContainerInfo;
  @Mock
  private Pipeline mockPipeline;

  private BlockManagerImpl blockManager;

  @BeforeEach
  void setUp() throws Exception {
    when(mockScm.getScmContext()).thenReturn(mockScmContext);
    when(mockScmContext.isInSafeMode()).thenReturn(false);

    PipelineID pipelineID = PipelineID.randomId();
    when(mockContainerInfo.getPipelineID()).thenReturn(pipelineID);
    when(mockContainerInfo.getContainerID()).thenReturn(1L);
    when(mockPipelineManager.getPipeline(pipelineID))
        .thenReturn(mockPipeline);
    when(mockPipeline.getId()).thenReturn(pipelineID);
    when(mockSequenceIdGen.getNextId(LOCAL_ID)).thenReturn(1L);

    blockManager = mock(BlockManagerImpl.class, CALLS_REAL_METHODS);
    setField(blockManager, "scm", mockScm);
    setField(blockManager, "writableContainerFactory", mockFactory);
    setField(blockManager, "pipelineManager", mockPipelineManager);
    setField(blockManager, "sequenceIdGen", mockSequenceIdGen);
    setField(blockManager, "containerSize", CONTAINER_SIZE);
  }

  @Test
  void testAllocateBlockFallsBackOnStorageTypeFailure() throws Exception {
    when(mockFactory.getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.SSD)))
        .thenThrow(new IOException("No SSD pipeline available"));
    when(mockFactory.getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.DISK)))
        .thenReturn(mockContainerInfo);

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(BlockManagerImpl.class);

    AllocatedBlock block = blockManager.allocateBlock(
        DEFAULT_BLOCK_SIZE, REPLICATION_CONFIG, OWNER,
        new ExcludeList(), StorageType.SSD);

    assertNotNull(block);
    verify(mockFactory).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.SSD));
    verify(mockFactory).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.DISK));
    assertThat(logCapturer.getOutput()).contains("falling back to DISK");
  }

  @Test
  void testAllocateBlockNoFallbackForDisk() throws Exception {
    when(mockFactory.getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.DISK)))
        .thenThrow(new IOException("No DISK pipeline available"));

    assertThrows(IOException.class,
        () -> blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
            REPLICATION_CONFIG, OWNER, new ExcludeList(), StorageType.DISK));

    verify(mockFactory).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.DISK));
    verify(mockFactory, never()).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.SSD));
  }

  @Test
  void testAllocateBlockPrimarySucceedsNoFallback() throws Exception {
    when(mockFactory.getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.SSD)))
        .thenReturn(mockContainerInfo);

    AllocatedBlock block = blockManager.allocateBlock(
        DEFAULT_BLOCK_SIZE, REPLICATION_CONFIG, OWNER,
        new ExcludeList(), StorageType.SSD);

    assertNotNull(block);
    verify(mockFactory).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.SSD));
    verify(mockFactory, never()).getContainer(
        anyLong(), any(), anyString(), any(), eq(StorageType.DISK));
  }

  private static void setField(Object target, String fieldName, Object value)
      throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
