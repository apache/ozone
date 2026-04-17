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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerCandidate;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This class tests for volume and container selection in DefaultContainerChoosingPolicy.
 */
public class TestDefaultContainerChoosingPolicy {

  @TempDir
  private Path baseDir;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final Set<State> DEFAULT_MOVABLE_STATES =
      CONF.getObject(DiskBalancerConfiguration.class).getMovableContainerStates();
  private static final long MB = 1024L * 1024L;
  private static final long VOLUME_CAPACITY = 2500L * MB; // 2500MB
  private static final long DEFAULT_CONTAINER_SIZE = 100L * MB; // 100MB
  private static final double THRESHOLD = 10.0;

  private ContainerChoosingPolicy policy;
  private MutableVolumeSet volumeSet;
  private String datanodeUuid;
  private Map<HddsVolume, Long> deltaMap;
  private OzoneContainer ozoneContainer;
  private ContainerSet containerSet;
  private Set<ContainerID> inProgressContainerIDs;

  @BeforeEach
  public void setup() throws Exception {
    datanodeUuid = UUID.randomUUID().toString();
    policy = new DefaultContainerChoosingPolicy(new ReentrantLock());
    deltaMap = new HashMap<>();
    inProgressContainerIDs = new HashSet<>();
  }

  // ---------- Volume selection test structure ----------

  /**
   * Test case data structure for volume configuration.
   */
  public static class VolumeTestConfig {
    private final String name;
    private final double utilization;
    private final Long customCapacity; // null means use default VOLUME_CAPACITY

    public VolumeTestConfig(String name, double utilization) {
      this.name = name;
      this.utilization = utilization;
      this.customCapacity = null;
    }

    public VolumeTestConfig(String name, double utilization, long customCapacity) {
      this.name = name;
      this.utilization = utilization;
      this.customCapacity = customCapacity;
    }

    public String getName() {
      return name;
    }

    public double getUtilization() {
      return utilization;
    }

    public Long getCustomCapacity() {
      return customCapacity;
    }
  }

  /**
   * Test scenario for chooseVolumesAndContainer.
   */
  public static class TestScenario {
    private final String name;
    private final List<VolumeTestConfig> volumes;
    private final double thresholdPercentage;
    private final long containerSize;
    private final boolean shouldFindPair;
    private final String expectedSourceDisk;
    private final String expectedDestinationDisk;
    private final Integer expectedSourceIndex;
    private final Integer expectedDestinationIndex;

    // Container selection: when non-null, create these on source instead of single container
    private final Map<Long, Long> sourceContainers;

    // When true, block lowest-util volume (simulate no space)
    private final boolean blockDestination;

    // Container IDs to add to inProgressContainerIDs before the call
    private final List<Long> inProgressIds;

    // When shouldFindPair, expected container ID (null = any)
    private final Long expectedContainerId;

    @SuppressWarnings("checkstyle:parameternumber")
    public TestScenario(String name, List<VolumeTestConfig> volumes, double thresholdPercentage,
        long containerSize, boolean shouldFindPair, String expectedSourceDisk, String expectedDestinationDisk,
        Integer expectedSourceIndex, Integer expectedDestinationIndex) {
      this(name, volumes, thresholdPercentage, containerSize, shouldFindPair,
          expectedSourceDisk, expectedDestinationDisk, expectedSourceIndex, expectedDestinationIndex,
          null, false, null, null);
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public TestScenario(String name, List<VolumeTestConfig> volumes, double thresholdPercentage,
        long containerSize, boolean shouldFindPair, String expectedSourceDisk, String expectedDestinationDisk,
        Integer expectedSourceIndex, Integer expectedDestinationIndex,
        Map<Long, Long> sourceContainers, boolean blockDestination, List<Long> inProgressIds,
        Long expectedContainerId) {
      this.name = name;
      this.volumes = volumes;
      this.thresholdPercentage = thresholdPercentage;
      this.containerSize = containerSize;
      this.shouldFindPair = shouldFindPair;
      this.expectedSourceDisk = expectedSourceDisk;
      this.expectedDestinationDisk = expectedDestinationDisk;
      this.expectedSourceIndex = expectedSourceIndex;
      this.expectedDestinationIndex = expectedDestinationIndex;
      this.sourceContainers = sourceContainers;
      this.blockDestination = blockDestination;
      this.inProgressIds = inProgressIds;
      this.expectedContainerId = expectedContainerId;
    }

    public String getName() {
      return name;
    }

    public List<VolumeTestConfig> getVolumes() {
      return volumes;
    }

    public double getThresholdPercentage() {
      return thresholdPercentage;
    }

    public long getContainerSize() {
      return containerSize;
    }

    public boolean shouldFindPair() {
      return shouldFindPair;
    }

    public String getExpectedSourceDisk() {
      return expectedSourceDisk;
    }

    public String getExpectedDestinationDisk() {
      return expectedDestinationDisk;
    }

    public Integer getExpectedSourceIndex() {
      return expectedSourceIndex;
    }

    public Integer getExpectedDestinationIndex() {
      return expectedDestinationIndex;
    }

    public Map<Long, Long> getSourceContainers() {
      return sourceContainers;
    }

    public boolean isBlockDestination() {
      return blockDestination;
    }

    public List<Long> getInProgressIds() {
      return inProgressIds;
    }

    public Long getExpectedContainerId() {
      return expectedContainerId;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  // ---------- Shared methods ----------

  private HddsVolume createVolume(String name, double utilization, long capacity)
      throws IOException {
    long usedSpace = (long) (capacity * utilization);
    Path volumePath = baseDir.resolve(name);

    // Create a configuration without reserved space to avoid capacity adjustments
    OzoneConfiguration volumeConf = new OzoneConfiguration();
    volumeConf.setFloat("hdds.datanode.dir.du.reserved.percent", 0.0f);

    SpaceUsageSource source = MockSpaceUsageSource.fixed(capacity,
        capacity - usedSpace);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);

    return new HddsVolume.Builder(volumePath.toString())
        .conf(volumeConf)
        .usageCheckFactory(factory)
        .build();
  }

  /**
   * Creates volumes from test configuration.
   *
   * @param configs List of volume configurations
   * @return List of created HddsVolumes
   */
  private List<HddsVolume> createVolumes(List<VolumeTestConfig> configs)
      throws IOException {
    List<HddsVolume> volumes = new ArrayList<>();
    for (VolumeTestConfig config : configs) {
      long capacity = config.getCustomCapacity() != null ?
          config.getCustomCapacity() : VOLUME_CAPACITY;
      HddsVolume volume = createVolume(config.getName(), config.getUtilization(), capacity);
      volumes.add(volume);
    }
    return volumes;
  }

  private void createContainer(long id, long usedBytes, HddsVolume vol) throws IOException {
    createContainer(id, usedBytes, vol, containerSet);
  }

  private void createContainer(long id, long usedBytes, HddsVolume vol, ContainerSet targetSet)
      throws IOException {
    createContainer(id, usedBytes, vol, targetSet, ContainerDataProto.State.CLOSED);
  }

  private void createContainer(long id, long usedBytes, HddsVolume vol, ContainerSet targetSet,
      ContainerDataProto.State state) throws IOException {
    long maxSize = usedBytes > 0 ? usedBytes : (long) CONF.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    KeyValueContainerData containerData = new KeyValueContainerData(id,
        ContainerLayoutVersion.FILE_PER_BLOCK, maxSize,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setState(state);
    containerData.setVolume(vol);
    containerData.getStatistics().setBlockBytesForTesting(usedBytes);
    KeyValueContainer container = new KeyValueContainer(containerData, CONF);
    targetSet.addContainer(container);
  }

  private void setupVolumeSetAndContainers(List<HddsVolume> volumes,
      HddsVolume sourceVolume, HddsVolume lowestVolume, TestScenario scenario) throws IOException {
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.set(HDDS_DATANODE_DIR_KEY, baseDir.resolve("defaultVolume").toString());
    volumeSet = new MutableVolumeSet(datanodeUuid, testConf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    // Use setVolumeMapForTesting to set only our test volumes
    // This replaces the entire volumeMap, removing any default volumes from configuration
    Map<String, StorageVolume> volumeMap = new HashMap<>();
    for (HddsVolume volume : volumes) {
      volumeMap.put(volume.getStorageDir().getAbsolutePath(), volume);
    }
    volumeSet.setVolumeMapForTesting(volumeMap);

    containerSet = newContainerSet();
    if (sourceVolume != null) {
      Map<Long, Long> containerSizes = scenario.getSourceContainers();
      if (containerSizes != null) {
        for (Map.Entry<Long, Long> e : containerSizes.entrySet()) {
          createContainer(e.getKey(), e.getValue(), sourceVolume);
        }
      } else {
        createContainer(1L, scenario.getContainerSize(), sourceVolume);
      }
    }

    if (scenario.isBlockDestination() && lowestVolume != null) {
      lowestVolume.incCommittedBytes(lowestVolume.getCurrentUsage().getAvailable());
    }
    if (scenario.getInProgressIds() != null) {
      for (Long id : scenario.getInProgressIds()) {
        inProgressContainerIDs.add(ContainerID.valueOf(id));
      }
    }
    mockContainerSet(containerSet);
  }

  private void mockContainerSet(ContainerSet cs) {
    ozoneContainer = mock(OzoneContainer.class);
    ContainerController controller = new ContainerController(cs, null);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(cs);
  }

  private MutableVolumeSet createVolumeSetForUsages(List<HddsVolume> volumes) throws IOException {
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.set(HDDS_DATANODE_DIR_KEY, baseDir.resolve("defaultVolume").toString());
    MutableVolumeSet vs = new MutableVolumeSet(datanodeUuid, testConf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    Map<String, StorageVolume> volumeMap = new HashMap<>();
    for (HddsVolume volume : volumes) {
      volumeMap.put(volume.getStorageDir().getAbsolutePath(), volume);
    }
    vs.setVolumeMapForTesting(volumeMap);
    return vs;
  }

  /**
   * When {@link DiskBalancerConfiguration#getContainerStates()} lists QUASI_CLOSED, the smallest
   * movable quasi-closed container may be chosen; with CLOSED only, only CLOSED replicas are
   * eligible.
   */
  @ParameterizedTest(name = "containerStates={0}")
  @MethodSource("quasiClosedEligibilityParams")
  public void testQuasiClosedEligibilityDependsOnContainerStates(String containerStates,
      long expectedContainerId)
      throws IOException {
    OzoneConfiguration testConf = new OzoneConfiguration();
    DiskBalancerConfiguration dbc = testConf.getObject(DiskBalancerConfiguration.class);
    dbc.setContainerStates(containerStates);
    testConf.setFromObject(dbc);
    ContainerChoosingPolicy policyUnderTest = new DefaultContainerChoosingPolicy(
        new ReentrantLock());

    inProgressContainerIDs.clear();
    deltaMap.clear();

    List<VolumeTestConfig> configs = Arrays.asList(
        new VolumeTestConfig("disk3", 0.15),
        new VolumeTestConfig("disk2", 0.85),
        new VolumeTestConfig("disk1", 0.90)
    );
    List<HddsVolume> volumes = createVolumes(configs);
    volumeSet = createVolumeSetForUsages(volumes);
    List<VolumeFixedUsage> sortedUsages = getVolumeUsages(volumeSet, deltaMap);
    sortedUsages.sort(Comparator.comparingDouble(VolumeFixedUsage::getUtilization));
    HddsVolume sourceVolume = sortedUsages.get(sortedUsages.size() - 1).getVolume();
    HddsVolume destVolume = sortedUsages.get(0).getVolume();

    containerSet = newContainerSet();
    createContainer(1L, 1500L * MB, sourceVolume, containerSet, ContainerDataProto.State.CLOSED);
    createContainer(2L, 50L * MB, sourceVolume, containerSet, ContainerDataProto.State.QUASI_CLOSED);
    createContainer(3L, 200L * MB, sourceVolume, containerSet, ContainerDataProto.State.CLOSED);

    mockContainerSet(containerSet);

    Set<State> movable = testConf.getObject(DiskBalancerConfiguration.class)
        .getMovableContainerStates();
    ContainerCandidate candidate = policyUnderTest.chooseVolumesAndContainer(ozoneContainer,
        volumeSet, deltaMap, inProgressContainerIDs, THRESHOLD, movable);

    assertNotNull(candidate);
    assertEquals(sourceVolume, candidate.getSourceVolume());
    assertEquals(destVolume, candidate.getDestVolume());
    assertEquals(expectedContainerId, candidate.getContainerData().getContainerID());
    if (expectedContainerId == 2L) {
      assertTrue(candidate.getContainerData().isQuasiClosed());
    } else {
      assertTrue(candidate.getContainerData().isClosed());
    }
  }

  private static Stream<Arguments> quasiClosedEligibilityParams() {
    return Stream.of(
        Arguments.arguments(DiskBalancerConfiguration.DEFAULT_CONTAINER_STATES, 2L),
        Arguments.arguments("CLOSED", 3L)
    );
  }

  /**
   * Generic test method that can be reused for different scenarios.
   *
   * @param scenario Test scenario configuration
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("testScenarios")
  public void testChooseVolumesAndContainer(TestScenario scenario) throws IOException {
    inProgressContainerIDs.clear();

    List<HddsVolume> volumes = createVolumes(scenario.getVolumes());
    List<VolumeFixedUsage> sortedUsages = getVolumeUsages(
        createVolumeSetForUsages(volumes), deltaMap);
    sortedUsages.sort(Comparator.comparingDouble(VolumeFixedUsage::getUtilization));
    HddsVolume sourceVolume = scenario.shouldFindPair() || scenario.getSourceContainers() != null
        ? sortedUsages.get(sortedUsages.size() - 1).getVolume() : null;
    HddsVolume lowestVolume = sortedUsages.isEmpty() ? null : sortedUsages.get(0).getVolume();
    setupVolumeSetAndContainers(volumes, sourceVolume, lowestVolume, scenario);

    // Create a map of disk names to volumes for verification
    Map<String, HddsVolume> diskNameToVolume = new HashMap<>();
    for (int i = 0; i < scenario.getVolumes().size(); i++) {
      diskNameToVolume.put(scenario.getVolumes().get(i).getName(), volumes.get(i));
    }

    // Get volume usages for verification
    List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, deltaMap);

    ContainerCandidate result = policy.chooseVolumesAndContainer(ozoneContainer,
        volumeSet, deltaMap, inProgressContainerIDs, scenario.getThresholdPercentage(),
        DEFAULT_MOVABLE_STATES);

    if (scenario.shouldFindPair()) {
      assertNotNull(result);
      assertNotNull(result.getSourceVolume());
      assertNotNull(result.getDestVolume());
      assertNotNull(result.getContainerData());

      // Verify source is the expected disk
      if (scenario.getExpectedSourceDisk() != null) {
        assertEquals(diskNameToVolume.get(scenario.getExpectedSourceDisk()), result.getSourceVolume());
      }

      // Verify destination is the expected disk (or one of the valid options)
      if (scenario.getExpectedDestinationDisk() != null) {
        assertEquals(diskNameToVolume.get(scenario.getExpectedDestinationDisk()), result.getDestVolume());
      }
      if (scenario.getExpectedContainerId() != null) {
        assertEquals(scenario.getExpectedContainerId(), (Long) result.getContainerData().getContainerID());
      }

      // Filter volumeUsages to only include volumes from our test scenario
      // This excludes any extra volumes that might be added from default configuration
      List<VolumeFixedUsage> testVolumeUsages = new ArrayList<>();
      for (VolumeFixedUsage usage : volumeUsages) {
        if (diskNameToVolume.containsValue(usage.getVolume())) {
          testVolumeUsages.add(usage);
        }
      }
      // Sort by utilization to ensure consistent ordering
      testVolumeUsages.sort(Comparator.comparingDouble(VolumeFixedUsage::getUtilization));

      // Verify source and destination indices match expected values
      // Since volumes in TestScenario are ordered from lowest to highest utilization,
      // and testVolumeUsages are sorted ascending (lowest to highest),
      // we can verify that the selected volumes are at the expected indices
      int sourceIndex = -1;
      int destIndex = -1;
      for (int i = 0; i < testVolumeUsages.size(); i++) {
        if (testVolumeUsages.get(i).getVolume().equals(result.getSourceVolume())) {
          sourceIndex = i;
        }
        if (testVolumeUsages.get(i).getVolume().equals(result.getDestVolume())) {
          destIndex = i;
        }
      }
      assertTrue(sourceIndex >= 0);
      assertTrue(destIndex >= 0);

      // Verify source is at the expected index (should be the highest utilization)
      if (scenario.getExpectedSourceIndex() != null) {
        assertEquals(scenario.getExpectedSourceIndex(), sourceIndex);
      }
      if (scenario.getExpectedDestinationIndex() != null) {
        assertEquals(scenario.getExpectedDestinationIndex(), destIndex);

        // Verify that destination has lower utilization than source
        double destUtilization = testVolumeUsages.get(destIndex).getUtilization();
        double sourceUtilization = testVolumeUsages.get(sourceIndex).getUtilization();
        assertTrue(destUtilization < sourceUtilization);

        // Verify that no volume with lower index (lower utilization) has sufficient space
        // If a volume at a lower index had sufficient space, it would have been chosen instead
        for (int i = 0; i < destIndex; i++) {
          assertTrue(scenario.getContainerSize() >=
              testVolumeUsages.get(i).computeUsableSpace());
        }
      }
    } else {
      assertNull(result);
    }
  }

  private static Map<Long, Long> mapOf(long... kvPairs) {
    Map<Long, Long> m = new HashMap<>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      m.put(kvPairs[i], kvPairs[i + 1]);
    }
    return m;
  }

  /**
   * Provides test scenarios for parameterized testing.
   */
  @SuppressWarnings("checkstyle:methodlength")
  public static Stream<Arguments> testScenarios() {
    return Stream.of(
        // <---------------------Volume selection Scenarios--------------------->

        // Scenario 1: One volume beyond threshold, no volumes under threshold
        // Disk1: 30%, Disk2: 30.1%, Disk3: 40%, Threshold: 5%
        // Ideal: 33.37%, Range: (28.37%, 38.37%), Out of range: Disk3
        // Expected source: Disk3 (highest) at index 2, Expected destination: Disk1 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "OneVolumeBeyondThresholdNoVolumesUnderThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.301),
                new VolumeTestConfig("disk3", 0.40)
            ), 5.0, DEFAULT_CONTAINER_SIZE, true, "disk3", "disk1", 2, 0)),

        // Scenario 2: Volumes both above and below threshold
        // Disk1: 90%, Disk2: 85%, Disk3: 15%, Threshold: 10%
        // Ideal: 63.33%, Range: (53.33%, 73.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "VolumesAboveAndBelowThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.15),
                new VolumeTestConfig("disk2", 0.85),
                new VolumeTestConfig("disk1", 0.90)
            ), 10.0, DEFAULT_CONTAINER_SIZE, true, "disk1", "disk3", 2, 0)),

        // Scenario 3: All volumes within threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 33%, Threshold: 10%
        // Ideal: 31%, Range: (21%, 41%), Out of range: None
        // Expected source: None, Expected destination: None
        Arguments.arguments(new TestScenario(
            "AllVolumesWithinThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.301),
                new VolumeTestConfig("disk3", 0.33)
            ), 10.0, DEFAULT_CONTAINER_SIZE, false, null, null, null, null)),

        // Scenario 4: One volume under threshold, no volumes above threshold
        // Disk1: 30%, Disk2: 30.1%, Disk3: 20%
        // Ideal: 26.70%, Range: (21.70%, 31.70%), Out of range: Disk3
        // Expected source: Disk2 (highest) at index 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "OneVolumeUnderThresholdNoVolumesAbove",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.20),
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.301)
            ), 5.0, DEFAULT_CONTAINER_SIZE, true, "disk2", "disk3", 2, 0)),

        // Scenario 5: Extreme imbalance - one very high, others very low
        // Disk1: 95%, Disk2: 5%, Disk3: 5.1%, Threshold: 10%
        // Ideal: 35.03%, Range: (25.03%, 45.03%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk2 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "ExtremeImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.05),
                new VolumeTestConfig("disk3", 0.051),
                new VolumeTestConfig("disk1", 0.95)
            ), 10.0, DEFAULT_CONTAINER_SIZE, true, "disk1", "disk2", 2, 0)),

        // Scenario 6: Multiple volumes above threshold, one below
        // Disk1: 80%, Disk2: 75%, Disk3: 20%, Threshold: 10%
        // Ideal: 58.33%, Range: (48.33%, 68.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "MultipleVolumesAboveOneBelow",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.20),
                new VolumeTestConfig("disk2", 0.75),
                new VolumeTestConfig("disk1", 0.80)
            ), 10.0, DEFAULT_CONTAINER_SIZE, true, "disk1", "disk3", 2, 0)),

        // Scenario 7: Edge case - volumes at threshold boundaries
        // Disk1: 50%, Disk2: 40%, Disk3: 60%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: Disk3 (at upper), Disk2 (at lower)
        // Expected source: Disk3 (highest) at index 2, Expected destination: Disk2 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "VolumesAtThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.40),
                new VolumeTestConfig("disk1", 0.50),
                new VolumeTestConfig("disk3", 0.60)
            ), 10.0, DEFAULT_CONTAINER_SIZE, true, "disk3", "disk2", 2, 0)),

        // Scenario 8: Small threshold with moderate imbalance
        // Disk1: 35%, Disk2: 30%, Disk3: 30.1%, Threshold: 2%
        // Ideal: 31.70%, Range: (29.70%, 33.70%), Out of range: Disk1
        // Container size must be small enough to fit on disk2 without exceeding threshold
        // disk2 after: (750MB + 50MB) / 2500MB = 32% < 33.7% threshold ✓
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk2 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "SmallThresholdModerateImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.30),
                new VolumeTestConfig("disk3", 0.301),
                new VolumeTestConfig("disk1", 0.35)
            ), 2.0, 50L * MB, true, "disk1", "disk2", 2, 0)),

        // Scenario 9: Best destination has low utilization but insufficient space
        // Disk1: 90% (Source)
        // Disk2: 10% (Best Util, but small capacity 500MB) -> REJECT (insufficient space)
        // Disk3: 20% (Second Best Util, plenty of space) -> ACCEPT
        // Disk2: capacity=500MB, 10% used=50MB, available=450MB
        // Container size 500MB > 450MB available, so disk2 is rejected
        // Disk3: capacity=2500MB, 20% used=500MB, available=2000MB
        // Container size 500MB < 2000MB available, so disk3 is accepted
        Arguments.arguments(new TestScenario(
            "BestDestInsufficientSpace",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.10, 500L * MB),
                new VolumeTestConfig("disk3", 0.20),
                new VolumeTestConfig("disk1", 0.90)
            ), 10.0, 500L * MB, true, "disk1", "disk3", 2, 1)),

        // Scenario 10: Volumes just inside threshold boundaries
        // Disk1: 40.01%, Disk2: 59.99%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: None (both are just inside)
        // Expected: No pair should be found as all volumes are within threshold
        Arguments.arguments(new TestScenario(
            "VolumesJustInsideThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.4001),
                new VolumeTestConfig("disk2", 0.5999)
            ), 10.0, DEFAULT_CONTAINER_SIZE, false, null, null, null, null)),

        // Scenario 10b: Volumes just outside threshold boundaries
        // Disk1: 39.99%, Disk2: 60.01%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: Disk1 (just below lower), Disk2 (just above upper)
        // Expected: Pair should be found - Disk2 as source at index 1, Disk1 as destination at index 0
        Arguments.arguments(new TestScenario(
            "VolumesJustOutsideThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.3999),
                new VolumeTestConfig("disk2", 0.6001)
            ), 10.0, DEFAULT_CONTAINER_SIZE, true, "disk2", "disk1", 1, 0)),

        // Scenario 11: No volumes have enough free space
        // Disk1: 90% (Source, highest)
        // Disk2: 10% (Destination candidate, but insufficient space)
        // Disk3: 20% (Destination candidate, but insufficient space)
        // Disk2: capacity=500MB, 10% used=50MB, available=450MB
        // Disk3: capacity=500MB, 20% used=100MB, available=400MB
        // Container size 500MB > available space on both disk2 and disk3, so both are rejected
        // No valid destination found
        Arguments.arguments(new TestScenario(
            "NoVolumesHaveEnoughFreeSpace",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.10, 500L * MB),
                new VolumeTestConfig("disk3", 0.20, 500L * MB),
                new VolumeTestConfig("disk1", 0.90)
            ), 10.0, 500L * MB, false, null, null, null, null)),

        // Scenario 12: Only one volume
        // Disk1: 80%
        // Cannot balance with only one volume
        // Expected: No pair should be found
        Arguments.arguments(new TestScenario(
            "OnlyOneVolume",
            Arrays.asList(new VolumeTestConfig("disk1", 0.80)),
            10.0, DEFAULT_CONTAINER_SIZE, false, null, null, null, null)),

        // Scenario 13: Zero volumes (empty volume set)
        // No volumes at all
        // Expected: No pair should be found
        Arguments.arguments(new TestScenario(
            "ZeroVolumes",
            Arrays.asList(), 10.0, DEFAULT_CONTAINER_SIZE, false, null, null, null, null)),

        // <---------------------Container selection Scenarios--------------------->

        // Container selection: multiple containers on source, first valid chosen
        Arguments.arguments(new TestScenario(
            "ContainerChosenSuccessfully",
            Arrays.asList(
                new VolumeTestConfig("dest-volume1", 0.10),
                new VolumeTestConfig("dest-volume2", 0.50),
                new VolumeTestConfig("source-volume", 0.80)
            ), THRESHOLD, DEFAULT_CONTAINER_SIZE, true, "source-volume", "dest-volume1", 2, 0,
            mapOf(1L, 500L * MB, 2L, 450L * MB, 3L, 200L * MB, 4L, 350L * MB, 5L, 500L * MB),
            false, null, 1L)),

        // Container selection: dest blocked, small containers in-progress, no fit
        Arguments.arguments(new TestScenario(
            "ContainerNotChosen",
            Arrays.asList(
                new VolumeTestConfig("dest-volume1", 0.10),
                new VolumeTestConfig("dest-volume2", 0.50),
                new VolumeTestConfig("source-volume", 0.80)
            ), THRESHOLD, DEFAULT_CONTAINER_SIZE, false, null, null, null, null,
            mapOf(1L, 500L * MB, 2L, 450L * MB, 3L, 200L * MB, 4L, 350L * MB, 5L, 500L * MB),
            true, Arrays.asList(3L, 4L), null)),

        // Container selection: size-zero containers skipped, first non-zero chosen
        Arguments.arguments(new TestScenario(
            "SizeZeroContainersSkipped",
            Arrays.asList(
                new VolumeTestConfig("dest-volume1", 0.10),
                new VolumeTestConfig("dest-volume2", 0.50),
                new VolumeTestConfig("source-volume", 0.80)
            ), THRESHOLD, DEFAULT_CONTAINER_SIZE, true, "source-volume", "dest-volume1", 2, 0,
            mapOf(10L, 0L, 11L, 0L, 12L, 200L * MB), false, null, 12L))
    );
  }
}
