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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the DefaultContainerChoosingPolicy.
 */
public class TestDefaultContainerChoosingPolicy {

  @TempDir
  private Path baseDir;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final long MB = 1024L * 1024L;
  private static final long VOLUME_CAPACITY = 2500L * MB; // 2500MB
  private static final double THRESHOLD = 10.0;

  private ContainerChoosingPolicy policy;
  private OzoneContainer ozoneContainer;
  private MutableVolumeSet volumeSet;
  private ContainerSet containerSet;
  private HddsVolume sourceVolume;
  private HddsVolume destVolume1;
  private HddsVolume destVolume2;
  private Set<ContainerID> inProgressContainerIDs;
  private Map<HddsVolume, Long> deltaMap;

  @BeforeEach
  public void setup() throws Exception {
    policy = new DefaultContainerChoosingPolicy();
    setupVolumesAndContainer();
    inProgressContainerIDs = new HashSet<>();
    deltaMap = new HashMap<>();
  }

  /**
   * Sets up a scenario with three volumes and five containers on the source.
   * Volumes:
   * - Source Volume: 80% used (2000MB)
   * - Destination Volume 1: 10% used (250MB)
   * - Destination Volume 2: 50% used (1250MB)
   * Containers on Source (Total 2000MB):
   * - C1: 500MB, C2: 450MB, C3: 200MB, C4: 350MB, C5: 500MB
   */
  private void setupVolumesAndContainer() throws IOException {
    // Create volumes with specific utilization
    sourceVolume = createVolume("source-volume", 0.80);
    destVolume1 = createVolume("dest-volume1", 0.10);
    destVolume2 = createVolume("dest-volume2", 0.50);

    // Create and spy on the volume set
    String datanodeUuid = UUID.randomUUID().toString();
    volumeSet = spy(new MutableVolumeSet(datanodeUuid, CONF, null,
        StorageVolume.VolumeType.DATA_VOLUME, null));
    when(volumeSet.getVolumesList())
        .thenReturn(Arrays.asList(sourceVolume, destVolume1, destVolume2));

    // Create five containers on the source volume
    containerSet = newContainerSet();
    Map<Long, Long> containerSizes = new HashMap<>();
    containerSizes.put(1L, 500L * MB);
    containerSizes.put(2L, 450L * MB);
    containerSizes.put(3L, 200L * MB);
    containerSizes.put(4L, 350L * MB);
    containerSizes.put(5L, 500L * MB);

    for (Map.Entry<Long, Long> entry : containerSizes.entrySet()) {
      createContainer(entry.getKey(), entry.getValue(), sourceVolume);
    }

    // Mock OzoneContainer to return our container set
    ozoneContainer = mock(OzoneContainer.class);
    ContainerController controller = new ContainerController(containerSet, null);
    when(ozoneContainer.getController()).thenReturn(controller);
  }

  /**
   * Create volumes with specific utilization.
   */
  private HddsVolume createVolume(String dir, double utilization)
      throws IOException {
    long usedSpace = (long) (VOLUME_CAPACITY * utilization);
    Path volumePath = baseDir.resolve(dir);

    SpaceUsageSource source = MockSpaceUsageSource.fixed(VOLUME_CAPACITY,
        VOLUME_CAPACITY - usedSpace);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);

    return new HddsVolume.Builder(volumePath.toString())
        .conf(CONF)
        .usageCheckFactory(factory)
        .build();
  }

  /**
   * Create KeyValueContainers and add it to the containerSet.
   */
  private void createContainer(long id, long size, HddsVolume vol)
      throws IOException {
    createContainer(id, size, vol, containerSet);
  }

  @Test
  public void testContainerChosenSuccessfully() {
    // The policy should choose the first productive container for destVolume1.
    // Total Used: 2000MB + 250MB + 1250MB = 3500MB
    // Total Capacity: 3 * 2500MB = 7500MB
    // Ideal Usage: 3500 / 7500 = ~46.67%
    // Max Allowed Utilization: 46.67% + 10% (threshold) = ~56.67%
    //
    // Evaluation for destVolume1 (10% used / 250MB):
    // - C1 (500MB) is productive: (250+500)/2500 = 30% <= 56.67%
    // The policy iterates by container ID, so it will find and return C1.

    ContainerData chosenContainer = policy.chooseContainer(ozoneContainer,
        sourceVolume, destVolume1, inProgressContainerIDs, THRESHOLD, volumeSet, deltaMap);

    // first container should be chosen
    assertNotNull(chosenContainer);
    assertEquals(1L, chosenContainer.getContainerID());
  }

  @Test
  public void testContainerNotChosen() {
    // For destVolume2, no container move should be productive.
    // Max Allowed Utilization is ~56.67%.
    //
    // Evaluation for destVolume2 (50% used / 1250MB):
    // Max productive size = (0.5667 * 2500) - 1250 = 166.75MB
    // All containers on the source volume (smallest is 200MB) are larger
    // than 166.75MB. Therefore, no container should be chosen.

    ContainerData chosenContainer = policy.chooseContainer(ozoneContainer,
        sourceVolume, destVolume2, inProgressContainerIDs, THRESHOLD, volumeSet, deltaMap);

    // No containers should not be chosen
    assertNull(chosenContainer);
  }

  @Test
  public void testSizeZeroContainersSkipped() throws IOException {
    // Create a new container set with containers that have size 0
    ContainerSet testContainerSet = newContainerSet();
    
    // Create containers with size 0 (should be skipped)
    createContainer(10L, 0L, sourceVolume, testContainerSet);
    createContainer(11L, 0L, sourceVolume, testContainerSet);
    
    // Create a container with non-zero size (should be chosen)
    createContainer(12L, 200L * MB, sourceVolume, testContainerSet);
    
    // Mock OzoneContainer to return our test container set
    OzoneContainer testOzoneContainer = mock(OzoneContainer.class);
    ContainerController testController = new ContainerController(testContainerSet, null);
    when(testOzoneContainer.getController()).thenReturn(testController);
    
    // The policy should skip containers 10 and 11 (size 0) and choose container 12
    ContainerData chosenContainer = policy.chooseContainer(testOzoneContainer,
        sourceVolume, destVolume1, inProgressContainerIDs, THRESHOLD, volumeSet, deltaMap);
    
    // Container 12 (non-zero size) should be chosen, skipping containers 10 and 11 (size 0)
    assertNotNull(chosenContainer);
    assertEquals(12L, chosenContainer.getContainerID());
    assertEquals(200L * MB, chosenContainer.getBytesUsed());
  }

  /**
   * Create KeyValueContainers and add it to the specified containerSet.
   * @param id container ID
   * @param usedBytes bytes used by the container (can be 0)
   * @param vol volume where container is located
   * @param targetContainerSet container set to add the container to
   */
  private void createContainer(long id, long usedBytes, HddsVolume vol,
      ContainerSet targetContainerSet) throws IOException {
    // Use maxSize as the container capacity (must be > 0)
    // If usedBytes is 0, we still need a valid maxSize for container creation
    long maxSize = usedBytes > 0 ? usedBytes : (long) CONF.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    KeyValueContainerData containerData = new KeyValueContainerData(id,
        ContainerLayoutVersion.FILE_PER_BLOCK, maxSize,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setState(ContainerDataProto.State.CLOSED);
    containerData.setVolume(vol);
    // Set the actual used bytes (can be 0)
    containerData.getStatistics().setBlockBytesForTesting(usedBytes);
    KeyValueContainer container = new KeyValueContainer(containerData, CONF);
    targetContainerSet.addContainer(container);
  }
}
