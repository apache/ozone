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
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for DefaultVolumeChoosingPolicy.
 */
public class TestDefaultVolumeChoosingPolicy {

  @TempDir
  private Path baseDir;

  private static final long MB = 1024L * 1024L;
  private static final long VOLUME_CAPACITY = 2500L * MB; // 2500MB - same for all volumes
  private static final long DEFAULT_CONTAINER_SIZE = 100L * MB; // 100MB
  private DefaultVolumeChoosingPolicy policy;
  private MutableVolumeSet volumeSet;
  private String datanodeUuid;
  private Map<HddsVolume, Long> deltaMap;

  @BeforeEach
  public void setup() {
    datanodeUuid = UUID.randomUUID().toString();
    policy = new DefaultVolumeChoosingPolicy(new ReentrantLock());
    deltaMap = new HashMap<>();
  }

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
   * Test scenario configuration.
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

    @SuppressWarnings("checkstyle:parameternumber")
    public TestScenario(String name, List<VolumeTestConfig> volumes, double thresholdPercentage,
        long containerSize, boolean shouldFindPair, String expectedSourceDisk, String expectedDestinationDisk,
        Integer expectedSourceIndex, Integer expectedDestinationIndex) {
      this.name = name;
      this.volumes = volumes;
      this.thresholdPercentage = thresholdPercentage;
      this.containerSize = containerSize;
      this.shouldFindPair = shouldFindPair;
      this.expectedSourceDisk = expectedSourceDisk;
      this.expectedDestinationDisk = expectedDestinationDisk;
      this.expectedSourceIndex = expectedSourceIndex;
      this.expectedDestinationIndex = expectedDestinationIndex;
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

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Creates a volume with specific utilization and capacity.
   *
   * @param name Volume name/identifier
   * @param utilization Utilization as a double (0.0 to 1.0, representing percentage)
   * @param capacity capacity for the volume
   * @return HddsVolume with specified utilization and capacity
   */
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

    HddsVolume volume = new HddsVolume.Builder(volumePath.toString())
        .conf(volumeConf)
        .usageCheckFactory(factory)
        .build();
    return volume;
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

  /**
   * Sets up volume set with given volumes.
   *
   * @param volumes List of volumes to add to volume set
   */
  private void setupVolumeSet(List<HddsVolume> volumes) throws IOException {
    // Use a clean configuration to avoid loading default volumes
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
  }

  /**
   * Generic test method that can be reused for different scenarios.
   *
   * @param scenario Test scenario configuration
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("testScenarios")
  public void testVolumeChoosingPolicy(TestScenario scenario)
      throws IOException {
    // Create volumes from configuration
    List<HddsVolume> volumes = createVolumes(scenario.getVolumes());
    setupVolumeSet(volumes);

    // Create a map of disk names to volumes for verification
    Map<String, HddsVolume> diskNameToVolume = new HashMap<>();
    for (int i = 0; i < scenario.getVolumes().size(); i++) {
      VolumeTestConfig config = scenario.getVolumes().get(i);
      diskNameToVolume.put(config.getName(), volumes.get(i));
    }

    // Get volume usages for verification
    List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, deltaMap);

    // Try to find a valid source-destination pair
    Pair<HddsVolume, HddsVolume> result = policy.chooseVolume(volumeSet,
        scenario.getThresholdPercentage(), deltaMap, scenario.getContainerSize());

    if (scenario.shouldFindPair()) {
      assertNotNull(result);
      assertNotNull(result.getLeft());
      assertNotNull(result.getRight());

      // Verify source is the expected disk
      if (scenario.getExpectedSourceDisk() != null) {
        HddsVolume expectedSource = diskNameToVolume.get(scenario.getExpectedSourceDisk());
        assertNotNull(expectedSource);
        assertEquals(expectedSource, result.getLeft());
      }

      // Verify destination is the expected disk (or one of the valid options)
      if (scenario.getExpectedDestinationDisk() != null) {
        HddsVolume expectedDest = diskNameToVolume.get(scenario.getExpectedDestinationDisk());
        assertNotNull(expectedDest);
        assertEquals(expectedDest, result.getRight());
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
        if (testVolumeUsages.get(i).getVolume().equals(result.getLeft())) {
          sourceIndex = i;
        }
        if (testVolumeUsages.get(i).getVolume().equals(result.getRight())) {
          destIndex = i;
        }
      }
      assertTrue(sourceIndex >= 0);
      assertTrue(destIndex >= 0);

      // Verify source is at the expected index (should be the highest utilization)
      if (scenario.getExpectedSourceIndex() != null) {
        assertEquals(scenario.getExpectedSourceIndex(), sourceIndex);

        // Verify that source has the highest utilization
        double sourceUtilization = testVolumeUsages.get(sourceIndex).getUtilization();
        for (int i = 0; i < testVolumeUsages.size(); i++) {
          if (i != sourceIndex) {
            double otherUtilization = testVolumeUsages.get(i).getUtilization();
            assertTrue(sourceUtilization >= otherUtilization);
          }
        }
      }

      // Verify destination is at the expected index (should be the lowest utilization among valid destinations)
      if (scenario.getExpectedDestinationIndex() != null) {
        assertEquals(scenario.getExpectedDestinationIndex(), destIndex);

        // Verify that destination has lower utilization than source
        double destUtilization = testVolumeUsages.get(destIndex).getUtilization();
        double sourceUtilization = testVolumeUsages.get(sourceIndex).getUtilization();
        assertTrue(destUtilization < sourceUtilization);

        // Verify that no volume with lower index (lower utilization) has sufficient space
        // If a volume at a lower index had sufficient space, it would have been chosen instead
        for (int i = 0; i < destIndex; i++) {
          VolumeFixedUsage lowerUtilUsage = testVolumeUsages.get(i);
          long usableSpace = lowerUtilUsage.computeUsableSpace();
          // Policy checks: containerSize < computeUsableSpace() to see if there's enough space
          // So if containerSize >= usableSpace, there's NOT enough space (correct - volume wasn't chosen)
          assertTrue(scenario.getContainerSize() >= usableSpace);
        }
      }
    } else {
      assertNull(result);
    }
  }

  /**
   * Provides test scenarios for parameterized testing.
   */
  @SuppressWarnings("checkstyle:methodlength")
  public static Stream<Arguments> testScenarios() {
    return Stream.of(
        // Scenario 1: One volume beyond threshold, no volumes under threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 40%, Threshold: 5%
        // Ideal: 33.33%, Range: (28.33%, 38.33%), Out of range: Disk3
        // Expected source: Disk3 (highest) at index 2, Expected destination: Disk1 or Disk2 (lowest) at index 0 or 1
        Arguments.arguments(new TestScenario(
            "OneVolumeBeyondThresholdNoVolumesUnderThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),  // Lowest utilization - index 0
                new VolumeTestConfig("disk2", 0.30),  // Same as disk1 - index 1
                new VolumeTestConfig("disk3", 0.40)   // Highest utilization - index 2
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk3",  // Expected source (highest)
            null,     // Destination can be disk1 or disk2 (both valid) so only check source
            2,        // Expected source index (highest utilization)
            null      // Destination index can be 0 or 1 (both have same utilization)
        )),

        // Scenario 2: Volumes both above and below threshold
        // Disk1: 90%, Disk2: 85%, Disk3: 15%, Threshold: 10%
        // Ideal: 63.33%, Range: (53.33%, 73.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "VolumesAboveAndBelowThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.15),  // Lowest utilization - index 0
                new VolumeTestConfig("disk2", 0.85),  // Middle utilization - index 1
                new VolumeTestConfig("disk1", 0.90)   // Highest utilization - index 2
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source (highest)
            "disk3",  // Expected destination (lowest)
            2,        // Expected source index (highest utilization)
            0         // Expected destination index (lowest utilization)
        )),

        // Scenario 3: All volumes within threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 33%, Threshold: 10%
        // Ideal: 31%, Range: (21%, 41%), Out of range: None
        // Expected source: None, Expected destination: None
        Arguments.arguments(new TestScenario(
            "AllVolumesWithinThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),  // Lowest utilization
                new VolumeTestConfig("disk2", 0.30),  // Same as disk1
                new VolumeTestConfig("disk3", 0.33)   // Highest utilization
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            false,
            null,     // No pair expected
            null,
            null,     // No source index expected
            null      // No destination index expected
        )),

        // Scenario 4: One volume under threshold, no volumes above threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 20%
        // Ideal: 26.67%, Range: (21.67%, 31.67%), Out of range: Disk3
        // Expected source: Disk1 or Disk2 (highest) at index 1 or 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "OneVolumeUnderThresholdNoVolumesAbove",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.20),  // Lowest utilization - index 0
                new VolumeTestConfig("disk1", 0.30),  // Middle utilization - index 1
                new VolumeTestConfig("disk2", 0.30)   // Highest utilization (tied with disk1) - index 2
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            null,     // Source can be disk1 or disk2 (both valid, highest)
            "disk3",  // Expected destination (lowest)
            null,     // Source index can be 1 or 2 (both have same utilization)
            0         // Expected destination index (lowest utilization)
        )),

        // Scenario 5: Extreme imbalance - one very high, others very low
        // Disk1: 95%, Disk2: 5%, Disk3: 5%, Threshold: 10%
        // Ideal: 35%, Range: (25%, 45%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk2 or Disk3 (lowest) at index 0 or 1
        Arguments.arguments(new TestScenario(
            "ExtremeImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.05),  // Lowest utilization - index 0
                new VolumeTestConfig("disk3", 0.05),  // Same as disk2 - index 1
                new VolumeTestConfig("disk1", 0.95)   // Highest utilization - index 2
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source (highest)
            null,     // Destination can be disk2 or disk3 (lowest)
            2,        // Expected source index (highest utilization)
            null      // Destination index can be 0 or 1 (both have same utilization)
        )),

        // Scenario 6: Multiple volumes above threshold, one below
        // Disk1: 80%, Disk2: 75%, Disk3: 20%, Threshold: 10%
        // Ideal: 58.33%, Range: (48.33%, 68.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk3 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "MultipleVolumesAboveOneBelow",
            Arrays.asList(
                new VolumeTestConfig("disk3", 0.20),  // Lowest utilization - index 0
                new VolumeTestConfig("disk2", 0.75),  // Middle utilization - index 1
                new VolumeTestConfig("disk1", 0.80)   // Highest utilization - index 2
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source (highest)
            "disk3",  // Expected destination (lowest)
            2,        // Expected source index (highest utilization)
            0         // Expected destination index (lowest utilization)
        )),

        // Scenario 7: Edge case - volumes at threshold boundaries
        // Disk1: 50%, Disk2: 40%, Disk3: 60%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: Disk3 (at upper), Disk2 (at lower)
        // Expected source: Disk3 (highest) at index 2, Expected destination: Disk2 (lowest) at index 0
        Arguments.arguments(new TestScenario(
            "VolumesAtThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.40),  // Lowest utilization - index 0
                new VolumeTestConfig("disk1", 0.50),  // Middle utilization - index 1
                new VolumeTestConfig("disk3", 0.60)   // Highest utilization - index 2
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk3",  // Expected source (highest)
            "disk2",  // Expected destination (lowest)
            2,        // Expected source index (highest utilization)
            0         // Expected destination index (lowest utilization)
        )),

        // Scenario 8: Small threshold with moderate imbalance
        // Disk1: 35%, Disk2: 30%, Disk3: 30%, Threshold: 2%
        // Ideal: 31.67%, Range: (29.67%, 33.67%), Out of range: Disk1
        // Expected source: Disk1 (highest) at index 2, Expected destination: Disk2 or Disk3 (lowest) at index 0 or 1
        Arguments.arguments(new TestScenario(
            "SmallThresholdModerateImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk2", 0.30),  // Lowest utilization - index 0
                new VolumeTestConfig("disk3", 0.30),  // Same as disk2 - index 1
                new VolumeTestConfig("disk1", 0.35)   // Highest utilization - index 2
            ),
            2.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source (highest)
            null,     // Destination can be disk2 or disk3 (lowest)
            2,        // Expected source index (highest utilization)
            null      // Destination index can be 0 or 1 (both have same utilization)
        )),

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
                new VolumeTestConfig("disk2", 0.10, 500L * MB), // Lowest utilization, small capacity - index 0
                new VolumeTestConfig("disk3", 0.20),  // Second lowest - index 1
                new VolumeTestConfig("disk1", 0.90)   // Highest utilization - index 2
            ),
            10.0,
            500L * MB, // Container size larger than disk2's available space
            true,
            "disk1",  // Expected source (highest)
            "disk3",  // Should skip disk2 and pick disk3
            2,        // Expected source index (highest utilization)
            1         // Expected destination index (disk3 at index 1, since disk2 at index 0 doesn't have enough space)
        )),

        // Scenario 10: Volumes just inside threshold boundaries
        // Disk1: 40.01%, Disk2: 59.99%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: None (both are just inside)
        // Expected: No pair should be found as all volumes are within threshold
        Arguments.arguments(new TestScenario(
            "VolumesJustInsideThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.4001), // Lowest utilization - index 0
                new VolumeTestConfig("disk2", 0.5999)  // Highest utilization - index 1
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            false,    // Should not find pair - both volumes are within threshold
            null,
            null,
            null,     // No source index expected
            null      // No destination index expected
        )),

        // Scenario 10b: Volumes just outside threshold boundaries
        // Disk1: 39.99%, Disk2: 60.01%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: Disk1 (just below lower), Disk2 (just above upper)
        // Expected: Pair should be found - Disk2 as source at index 1, Disk1 as destination at index 0
        Arguments.arguments(new TestScenario(
            "VolumesJustOutsideThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.3999), // Lowest utilization - index 0
                new VolumeTestConfig("disk2", 0.6001)  // Highest utilization - index 1
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,     // Should find pair - volumes are outside threshold
            "disk2",  // Expected source (highest utilization)
            "disk1",  // Expected destination (lowest utilization)
            1,        // Expected source index (highest utilization)
            0         // Expected destination index (lowest utilization)
        )),

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
                new VolumeTestConfig("disk2", 0.10, 500L * MB), // Lowest utilization - index 0
                new VolumeTestConfig("disk3", 0.20, 500L * MB), // Second lowest - index 1
                new VolumeTestConfig("disk1", 0.90)   // Highest utilization - index 2
            ),
            10.0,
            500L * MB, // Container size larger than both disk2's and disk3's available space
            false,     // Should not find pair - no destination has enough space
            null,
            null,
            null,     // No source index expected
            null      // No destination index expected
        )),

        // Scenario 12: Only one volume
        // Disk1: 80%
        // Cannot balance with only one volume
        // Expected: No pair should be found
        Arguments.arguments(new TestScenario(
            "OnlyOneVolume",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.80)  // Only volume
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            false,    // Should not find pair - need at least 2 volumes
            null,
            null,
            null,     // No source index expected
            null      // No destination index expected
        )),

        // Scenario 13: Zero volumes (empty volume set)
        // No volumes at all
        // Expected: No pair should be found
        Arguments.arguments(new TestScenario(
            "ZeroVolumes",
            Arrays.asList(), // Empty list
            10.0,
            DEFAULT_CONTAINER_SIZE,
            false,    // Should not find pair - no volumes available
            null,
            null,
            null,     // No source index expected
            null      // No destination index expected
        ))
    );
  }
}

