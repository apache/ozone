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

import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.UUID;
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

import java.util.stream.Stream;

/**
 * Unit tests for DefaultVolumeChoosingPolicy.
 * Tests scenarios where one volume is beyond threshold and no volumes are under threshold.
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

    public VolumeTestConfig(String name, double utilization) {
      this.name = name;
      this.utilization = utilization;
    }

    public String getName() {
      return name;
    }

    public double getUtilization() {
      return utilization;
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

    public TestScenario(String name, List<VolumeTestConfig> volumes, double thresholdPercentage,
                       long containerSize, boolean shouldFindPair,
                       String expectedSourceDisk, String expectedDestinationDisk) {
      this.name = name;
      this.volumes = volumes;
      this.thresholdPercentage = thresholdPercentage;
      this.containerSize = containerSize;
      this.shouldFindPair = shouldFindPair;
      this.expectedSourceDisk = expectedSourceDisk;
      this.expectedDestinationDisk = expectedDestinationDisk;
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

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Creates a volume with specific utilization.
   *
   * @param name Volume name/identifier
   * @param utilization Utilization as a double (0.0 to 1.0, representing percentage)
   * @return HddsVolume with specified utilization
   */
  private HddsVolume createVolume(String name, double utilization)
      throws IOException {
    long usedSpace = (long) (VOLUME_CAPACITY * utilization);
    Path volumePath = baseDir.resolve(name);

    // Create a configuration without reserved space to avoid capacity adjustments
    OzoneConfiguration volumeConf = new OzoneConfiguration();
    volumeConf.setFloat("hdds.datanode.dir.du.reserved.percent", 0.0f);

    SpaceUsageSource source = MockSpaceUsageSource.fixed(VOLUME_CAPACITY,
        VOLUME_CAPACITY - usedSpace);
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
      HddsVolume volume = createVolume(config.getName(), config.getUtilization());
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
    // Create volume set without any default volumes from configuration
    // Use a clean configuration to avoid loading default volumes
    OzoneConfiguration testConf = new OzoneConfiguration();
    // Explicitly set HDDS_DATANODE_DIR_KEY to empty to prevent default volumes
    testConf.set("hdds.datanode.dir.key", "");
    volumeSet = new MutableVolumeSet(datanodeUuid, testConf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    
    // Use setVolumeMapForTesting to set only our test volumes
    // This ensures no default volumes from configuration are included
    Map<String, StorageVolume> volumeMap = new HashMap<>();
    for (HddsVolume volume : volumes) {
      volumeMap.put(volume.getStorageDir().getAbsolutePath(), volume);
    }
    volumeSet.setVolumeMapForTesting(volumeMap);
  }

  /**
   * Helper method to get disk name from volume.
   */
  private String getDiskName(HddsVolume volume, Map<String, HddsVolume> diskNameToVolume) {
    for (Map.Entry<String, HddsVolume> entry : diskNameToVolume.entrySet()) {
      if (entry.getValue().equals(volume)) {
        return entry.getKey();
      }
    }
    return "unknown";
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
      assertNotNull(result, "Should find a valid source-destination pair for scenario");
      assertNotNull(result.getLeft(), "Source volume should not be null");
      assertNotNull(result.getRight(), "Destination volume should not be null");

      // Verify source is the expected disk
      if (scenario.getExpectedSourceDisk() != null) {
        HddsVolume expectedSource = diskNameToVolume.get(scenario.getExpectedSourceDisk());
        assertNotNull(expectedSource, "Expected source disk not found: " + scenario.getExpectedSourceDisk());
        assertTrue(result.getLeft().equals(expectedSource),
            "Expected source disk: " + scenario.getExpectedSourceDisk() + 
            ", but got: " + getDiskName(result.getLeft(), diskNameToVolume));
      }

      // Verify destination is the expected disk (or one of the valid options)
      if (scenario.getExpectedDestinationDisk() != null) {
        HddsVolume expectedDest = diskNameToVolume.get(scenario.getExpectedDestinationDisk());
        assertNotNull(expectedDest, "Expected destination disk not found: " + scenario.getExpectedDestinationDisk());
        assertTrue(result.getRight().equals(expectedDest),
            "Expected destination disk: " + scenario.getExpectedDestinationDisk() + 
            ", but got: " + getDiskName(result.getRight(), diskNameToVolume));
      }

      // Verify source is the highest utilization volume
      VolumeFixedUsage sourceUsage = null;
      for (VolumeFixedUsage usage : volumeUsages) {
        if (usage.getVolume().equals(result.getLeft())) {
          sourceUsage = usage;
          break;
        }
      }
      assertNotNull(sourceUsage, "Source usage should be found");

      // Verify destination has lower utilization than source
      VolumeFixedUsage destUsage = null;
      for (VolumeFixedUsage usage : volumeUsages) {
        if (usage.getVolume().equals(result.getRight())) {
          destUsage = usage;
          break;
        }
      }
      assertNotNull(destUsage, "Destination usage should be found");
      assertTrue(destUsage.getUtilization() < sourceUsage.getUtilization(),
          "Destination should have lower utilization than source");
    } else {
      assertNull(result, "Should not find a valid pair for this scenario");
    }
  }

  /**
   * Provides test scenarios for parameterized testing.
   */
  public static Stream<Arguments> testScenarios() {
    return Stream.of(
        // Scenario 1: One volume beyond threshold, no volumes under threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 40%, Threshold: 5%
        // Ideal: 33.33%, Range: (28.33%, 38.33%), Out of range: Disk3
        // Expected source: Disk3, Expected destination: Disk1 or Disk2
        Arguments.arguments(new TestScenario(
            "OneVolumeBeyondThresholdNoVolumesUnderThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.30),
                new VolumeTestConfig("disk3", 0.40)
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk3",  // Expected source
            null      // Destination can be disk1 or disk2 (both valid) so only check source
        )),

        // Scenario 2: Volumes both above and below threshold
        // Disk1: 90%, Disk2: 85%, Disk3: 15%, Threshold: 10%
        // Ideal: 63.33%, Range: (53.33%, 73.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest), Expected destination: Disk3 (lowest)
        Arguments.arguments(new TestScenario(
            "VolumesAboveAndBelowThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.90),
                new VolumeTestConfig("disk2", 0.85),
                new VolumeTestConfig("disk3", 0.15)
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source
            "disk3"   // Expected destination
        )),

        // Scenario 3: All volumes within threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 33%, Threshold: 10%
        // Ideal: 31%, Range: (21%, 41%), Out of range: None
        // Expected source: None, Expected destination: None
        Arguments.arguments(new TestScenario(
            "AllVolumesWithinThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.30),
                new VolumeTestConfig("disk3", 0.33)
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            false,
            null,     // No pair expected
            null
        )),

        // Scenario 4: One volume under threshold, no volumes above threshold
        // Disk1: 30%, Disk2: 30%, Disk3: 20%
        // Ideal: 26.67%, Range: (21.67%, 31.67%), Out of range: Disk3
        // Expected source: Disk1 or Disk2, Expected destination: Disk3
        Arguments.arguments(new TestScenario(
            "OneVolumeUnderThresholdNoVolumesAbove",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.30),
                new VolumeTestConfig("disk2", 0.30),
                new VolumeTestConfig("disk3", 0.20)
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            null,     // Source can be disk1 or disk2 (both valid)
            "disk3"    // Expected destination
        )),

        // Scenario 5: Extreme imbalance - one very high, others very low
        // Disk1: 95%, Disk2: 5%, Disk3: 5%, Threshold: 10%
        // Ideal: 35%, Range: (25%, 45%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1, Expected destination: Disk2 or Disk3
        Arguments.arguments(new TestScenario(
            "ExtremeImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.95),
                new VolumeTestConfig("disk2", 0.05),
                new VolumeTestConfig("disk3", 0.05)
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source
            null      // Destination can be disk2 or disk3
        )),

        // Scenario 6: Multiple volumes above threshold, one below
        // Disk1: 80%, Disk2: 75%, Disk3: 20%, Threshold: 10%
        // Ideal: 58.33%, Range: (48.33%, 68.33%), Out of range: Disk1, Disk2, Disk3
        // Expected source: Disk1 (highest), Expected destination: Disk3 (lowest)
        Arguments.arguments(new TestScenario(
            "MultipleVolumesAboveOneBelow",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.80),
                new VolumeTestConfig("disk2", 0.75),
                new VolumeTestConfig("disk3", 0.20)
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source
            "disk3"    // Expected destination
        )),

        // Scenario 7: All volumes above threshold
        // Disk1: 80%, Disk2: 85%, Disk3: 90%, Threshold: 5%
        // Ideal: 85%, Range: (80%, 90%), Out of range: Disk3 (at boundary)
        // Expected source: Disk3 (highest), Expected destination: Disk1 (lowest)
        Arguments.arguments(new TestScenario(
            "AllVolumesAboveThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.80),
                new VolumeTestConfig("disk2", 0.85),
                new VolumeTestConfig("disk3", 0.90)
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk3",  // Expected source
            "disk1"    // Expected destination
        )),

        // Scenario 8: All volumes below threshold
        // Disk1: 10%, Disk2: 15%, Disk3: 5%, Threshold: 5%
        // Ideal: 10%, Range: (5%, 15%), Out of range: Disk3 (at boundary)
        // Expected source: Disk2 (highest), Expected destination: Disk3 (lowest)
        Arguments.arguments(new TestScenario(
            "AllVolumesBelowThreshold",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.10),
                new VolumeTestConfig("disk2", 0.15),
                new VolumeTestConfig("disk3", 0.05)
            ),
            5.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk2",  // Expected source
            "disk3"    // Expected destination
        )),

        // Scenario 9: Edge case - volumes at threshold boundaries
        // Disk1: 50%, Disk2: 40%, Disk3: 60%, Threshold: 10%
        // Ideal: 50%, Range: (40%, 60%), Out of range: Disk3 (at upper), Disk2 (at lower)
        // Expected source: Disk3 (highest), Expected destination: Disk2 (lowest)
        Arguments.arguments(new TestScenario(
            "VolumesAtThresholdBoundaries",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.50),
                new VolumeTestConfig("disk2", 0.40),
                new VolumeTestConfig("disk3", 0.60)
            ),
            10.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk3",  // Expected source
            "disk2"    // Expected destination
        )),

        // Scenario 10: Small threshold with moderate imbalance
        // Disk1: 35%, Disk2: 30%, Disk3: 30%, Threshold: 2%
        // Ideal: 31.67%, Range: (29.67%, 33.67%), Out of range: Disk1
        // Expected source: Disk1, Expected destination: Disk2 or Disk3
        Arguments.arguments(new TestScenario(
            "SmallThresholdModerateImbalance",
            Arrays.asList(
                new VolumeTestConfig("disk1", 0.35),
                new VolumeTestConfig("disk2", 0.30),
                new VolumeTestConfig("disk3", 0.30)
            ),
            2.0,
            DEFAULT_CONTAINER_SIZE,
            true,
            "disk1",  // Expected source
            null      // Destination can be disk2 or disk3
        ))
    );
  }
}

