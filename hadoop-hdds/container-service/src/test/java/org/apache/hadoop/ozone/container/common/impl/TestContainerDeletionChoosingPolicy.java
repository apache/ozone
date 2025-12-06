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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService.ContainerBlockInfo;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * The class for testing container deletion choosing policy.
 */
public class TestContainerDeletionChoosingPolicy {
  @TempDir
  private File tempFile;
  private String path;
  private ContainerSet containerSet;
  private OzoneConfiguration conf;
  // the service timeout
  private static final int SERVICE_TIMEOUT_IN_MILLISECONDS = 0;
  private static final int SERVICE_INTERVAL_IN_MILLISECONDS = 1000;

  @BeforeEach
  public void init() throws Throwable {
    conf = new OzoneConfiguration();
    path = tempFile.getPath();
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testRandomChoosingPolicy(ContainerLayoutVersion layout)
      throws IOException {
    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }
    assertTrue(containerDir.mkdirs());

    conf.set(
        ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
        RandomContainerDeletionChoosingPolicy.class.getName());
    containerSet = newContainerSet();

    int numContainers = 10;
    for (int i = 0; i < numContainers; i++) {
      KeyValueContainerData data = new KeyValueContainerData(i,
          layout,
          ContainerTestHelper.CONTAINER_MAX_SIZE, UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
      data.incrPendingDeletionBlocks(20, 256);
      data.closeContainer();
      KeyValueContainer container = new KeyValueContainer(data, conf);
      containerSet.addContainer(container);
      assertThat(
          containerSet.getContainerMapCopy())
              .containsKey(data.getContainerID());
    }
    BlockDeletingService blockDeletingService = getBlockDeletingService();

    int blockLimitPerInterval = 5;
    ContainerDeletionChoosingPolicy deletionPolicy =
        new RandomContainerDeletionChoosingPolicy();
    List<ContainerBlockInfo> result0 = blockDeletingService
        .chooseContainerForBlockDeletion(blockLimitPerInterval, deletionPolicy);

    long totPendingBlocks = 0;
    for (ContainerBlockInfo pr : result0) {
      totPendingBlocks += pr.getNumBlocksToDelete();
    }
    assertThat(totPendingBlocks).isGreaterThanOrEqualTo(blockLimitPerInterval);

    // test random choosing. We choose 100 times the 3 datanodes twice.
    //We expect different order at least once.
    for (int j = 0; j < 100; j++) {
      List<ContainerBlockInfo> result1 = blockDeletingService
              .chooseContainerForBlockDeletion(50, deletionPolicy);
      List<ContainerBlockInfo> result2 = blockDeletingService
              .chooseContainerForBlockDeletion(50, deletionPolicy);
      boolean hasShuffled = false;
      for (int i = 0; i < result1.size(); i++) {
        if (result1.get(i).getContainerData().getContainerID() != result2.get(i)
            .getContainerData().getContainerID()) {
          return;
        }
      }
    }
    fail("Chosen container results were same 100 times");

  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testBlockDeletionAllowedAndDisallowedStates(ContainerLayoutVersion layout)
      throws IOException {
    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }
    assertTrue(containerDir.mkdirs());

    conf.set(
        ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
        TopNOrderedContainerDeletionChoosingPolicy.class.getName());
    containerSet = newContainerSet();

    // Helper to create container with given state and blocks
    KeyValueContainerData closedData = createContainerWithState(layout,
        ContainerProtos.ContainerDataProto.State.CLOSED);
    KeyValueContainerData quasiClosedData = createContainerWithState(layout,
        ContainerProtos.ContainerDataProto.State.QUASI_CLOSED);
    KeyValueContainerData openData = createContainerWithState(layout,
        ContainerProtos.ContainerDataProto.State.OPEN);
    KeyValueContainerData closingData = createContainerWithState(layout,
        ContainerProtos.ContainerDataProto.State.CLOSING);

    BlockDeletingService blockDeletingService = getBlockDeletingService();
    ContainerDeletionChoosingPolicy deletionPolicy =
        new TopNOrderedContainerDeletionChoosingPolicy();

    List<ContainerBlockInfo> result = blockDeletingService
        .chooseContainerForBlockDeletion(20, deletionPolicy);

    List<Long> selectedIds = result.stream()
        .map(info -> info.getContainerData().getContainerID())
        .collect(Collectors.toList());
    // Allowed states
    assertTrue(selectedIds.contains(closedData.getContainerID()),
        "CLOSED container must be selected for block deletion.");
    assertTrue(selectedIds.contains(quasiClosedData.getContainerID()),
        "QUASI_CLOSED container must be selected for block deletion.");

    // Disallowed states
    assertFalse(selectedIds.contains(openData.getContainerID()),
        "OPEN container must NOT be selected for block deletion.");
    assertFalse(selectedIds.contains(closingData.getContainerID()),
        "CLOSING container must NOT be selected for block deletion.");
  }

  private KeyValueContainerData createContainerWithState(
      ContainerLayoutVersion layout,
      ContainerProtos.ContainerDataProto.State state) throws IOException {

    long containerId = RandomUtils.secure().randomLong();
    KeyValueContainerData data = new KeyValueContainerData(
        containerId, layout, ContainerTestHelper.CONTAINER_MAX_SIZE,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());

    data.incrPendingDeletionBlocks(5, 5 * 256);
    data.setState(state);
    containerSet.addContainer(new KeyValueContainer(data, conf));

    assertThat(containerSet.getContainerMapCopy()).containsKey(containerId);
    return data;
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testTopNOrderedChoosingPolicy(ContainerLayoutVersion layout)
      throws IOException {
    File containerDir = new File(path);
    if (containerDir.exists()) {
      FileUtils.deleteDirectory(new File(path));
    }
    assertTrue(containerDir.mkdirs());

    conf.set(
        ScmConfigKeys.OZONE_SCM_KEY_VALUE_CONTAINER_DELETION_CHOOSING_POLICY,
        TopNOrderedContainerDeletionChoosingPolicy.class.getName());
    containerSet = newContainerSet();

    int numContainers = 10;
    Random random = new Random();
    Map<Long, Integer> name2Count = new HashMap<>();
    List<Integer> numberOfBlocks = new ArrayList<Integer>();
    // create [numContainers + 1] containers
    for (int i = 0; i <= numContainers; i++) {
      long containerId = RandomUtils.secure().randomLong();
      KeyValueContainerData data =
          new KeyValueContainerData(containerId,
              layout,
              ContainerTestHelper.CONTAINER_MAX_SIZE,
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString());
      if (i != numContainers) {
        int deletionBlocks = random.nextInt(numContainers) + 1;
        numberOfBlocks.add(deletionBlocks);
        data.incrPendingDeletionBlocks(deletionBlocks, 256);
        name2Count.put(containerId, deletionBlocks);
      }
      KeyValueContainer container = new KeyValueContainer(data, conf);
      data.closeContainer();
      containerSet.addContainer(container);
      assertThat(containerSet.getContainerMapCopy()).containsKey(containerId);
    }
    numberOfBlocks.sort(Collections.reverseOrder());
    int blockLimitPerInterval = 5;
    BlockDeletingService blockDeletingService = getBlockDeletingService();
    ContainerDeletionChoosingPolicy deletionPolicy =
        new TopNOrderedContainerDeletionChoosingPolicy();
    List<ContainerBlockInfo> result0 = blockDeletingService
        .chooseContainerForBlockDeletion(blockLimitPerInterval, deletionPolicy);
    long totPendingBlocks = 0;
    for (ContainerBlockInfo pr : result0) {
      totPendingBlocks += pr.getNumBlocksToDelete();
    }
    assertThat(totPendingBlocks).isGreaterThanOrEqualTo(blockLimitPerInterval);


    List<ContainerBlockInfo> result1 = blockDeletingService
        .chooseContainerForBlockDeletion(numContainers + 1, deletionPolicy);
    // the empty deletion blocks container should not be chosen
    int containerCount = 0;
    int c = 0;
    for (Integer numberOfBlock : numberOfBlocks) {
      containerCount++;
      c = c + numberOfBlock;
      if (c >= (numContainers + 1)) {
        break;
      }
    }
    assertEquals(containerCount, result1.size());

    // verify the order of return list
    int initialName2CountSize = name2Count.size();
    int lastCount = Integer.MAX_VALUE;
    for (ContainerBlockInfo data : result1) {
      int currentCount =
          name2Count.remove(data.getContainerData().getContainerID());
      // previous count should not smaller than next one
      assertThat(currentCount).isGreaterThan(0).isLessThanOrEqualTo(lastCount);
      lastCount = currentCount;
    }
    // ensure all the container data are compared
    assertEquals(result1.size(),
        initialName2CountSize - name2Count.size());
  }

  private BlockDeletingService getBlockDeletingService() {
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    BlockDeletingService blockDeletingService = new BlockDeletingService(ozoneContainer,
        SERVICE_INTERVAL_IN_MILLISECONDS, SERVICE_TIMEOUT_IN_MILLISECONDS,
        TimeUnit.MILLISECONDS, 10, conf, new ContainerChecksumTreeManager(conf));
    return blockDeletingService;

  }
}
