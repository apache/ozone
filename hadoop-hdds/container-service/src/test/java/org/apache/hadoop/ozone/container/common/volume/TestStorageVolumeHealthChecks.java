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

package org.apache.hadoop.ozone.container.common.volume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.utils.SlidingWindow;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DiskCheckUtil;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for StorageVolume health checks using Real volume instances with
 * mocked checkers to simulate failures.
 */
public class TestStorageVolumeHealthChecks {

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @TempDir
  private static Path volumePath;

  public static Stream<Arguments> volumeBuilders() {
    HddsVolume.Builder hddsVolumeBuilder =
        new HddsVolume.Builder(volumePath.toString())
            .datanodeUuid(DATANODE_UUID)
            .conf(CONF)
            .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);

    MetadataVolume.Builder metadataVolumeBuilder =
        new MetadataVolume.Builder(volumePath.toString())
            .datanodeUuid(DATANODE_UUID)
            .conf(CONF)
            .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);

    DbVolume.Builder dbVolumeBuilder =
        new DbVolume.Builder(volumePath.toString())
            .datanodeUuid(DATANODE_UUID)
            .conf(CONF)
            .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);

    return Stream.of(
        Arguments.of(Named.of("HDDS Volume", hddsVolumeBuilder)),
        Arguments.of(Named.of("Metadata Volume", metadataVolumeBuilder)),
        Arguments.of(Named.of("DB Volume", dbVolumeBuilder))
    );
  }

  @BeforeEach
  public void setup() throws Exception {
    // Volume path must be static to construct volume argument provider, but
    // needs to be cleared before each test.
    FileUtils.deleteDirectory(volumePath.toFile());
    DiskCheckUtil.clearTestImpl();
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckExistence(StorageVolume.Builder<?> builder)
      throws Exception {
    StorageVolume volume = builder.build();
    volume.format(CLUSTER_ID);
    volume.createTmpDirs(CLUSTER_ID);

    VolumeCheckResult result = volume.check(false);
    assertEquals(VolumeCheckResult.HEALTHY, result);

    final DiskCheckUtil.DiskChecks doesNotExist =
        new DiskCheckUtil.DiskChecks() {
          @Override
          public boolean checkExistence(File storageDir) {
            return false;
          }
        };

    DiskCheckUtil.setTestImpl(doesNotExist);
    result = volume.check(false);
    assertEquals(VolumeCheckResult.FAILED, result);
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testVolumeFullHealth(StorageVolume.Builder<?> builder) throws Exception {
    verifyFullVolumeHealthWithDiskReadWriteStatus(builder, true, false);
  }

  public void verifyFullVolumeHealthWithDiskReadWriteStatus(StorageVolume.Builder<?> builder, boolean... checkResult)
      throws Exception {

    for (boolean result : checkResult) {
      StorageVolume volume = builder.build();

      VolumeUsage usage = volume.getVolumeUsage();
      DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
      int minimumDiskSpace = dnConf.getVolumeHealthCheckFileSize() * 2;
      // Keep remaining space as just less than double of VolumeHealthCheckFileSize.
      usage.incrementUsedSpace(usage.getCurrentUsage().getAvailable() - minimumDiskSpace + 1);
      usage.realUsage();
      DiskCheckUtil.DiskChecks ioFailure = new DiskCheckUtil.DiskChecks() {
        @Override
        public boolean checkReadWrite(File storageDir, File testFileDir,
                                      int numBytesToWrite) {
          return result;
        }
      };
      DiskCheckUtil.setTestImpl(ioFailure);
      // Volume will remain healthy as volume don't have enough space to check READ/WRITE
      assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
      // Even in second try volume will remain HEALTHY
      assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));

      // Now keep enough space for read/write check to go through
      usage.decrementUsedSpace(minimumDiskSpace + 1);

      // volumeIOFailureTolerance is 1, so first time it will be HEALTHY always
      assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
      if (result) {
        // Volume will remain as healthy as READ/WRITE check is fine
        assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
      } else {
        // Second time volume will fail as READ/WRITE check has failed
        assertEquals(VolumeCheckResult.FAILED, volume.check(false));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckPermissions(StorageVolume.Builder<?> builder)
      throws Exception {
    StorageVolume volume = builder.build();
    volume.format(CLUSTER_ID);
    volume.createTmpDirs(CLUSTER_ID);

    VolumeCheckResult result = volume.check(false);
    assertEquals(VolumeCheckResult.HEALTHY, result);

    final DiskCheckUtil.DiskChecks noPermissions =
        new DiskCheckUtil.DiskChecks() {
          @Override
          public boolean checkPermissions(File storageDir) {
            return false;
          }
        };

    DiskCheckUtil.setTestImpl(noPermissions);
    result = volume.check(false);
    assertEquals(VolumeCheckResult.FAILED, result);
  }

  /**
   * Setting test count to 0 should disable IO tests.
   */
  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckIODisabled(StorageVolume.Builder<?> builder)
      throws Exception {
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    dnConf.setVolumeIOTestCount(0);
    CONF.setFromObject(dnConf);

    builder.conf(CONF);
    StorageVolume volume = builder.build();
    volume.format(CLUSTER_ID);
    volume.createTmpDirs(CLUSTER_ID);

    DiskCheckUtil.DiskChecks ioFailure = new DiskCheckUtil.DiskChecks() {
          @Override
          public boolean checkReadWrite(File storageDir, File testFileDir,
                                        int numBytesToWrite) {
            return false;
          }
        };
    DiskCheckUtil.setTestImpl(ioFailure);
    assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
  }

  @Test
  public void testCheckIODefaultConfigs() {
    CONF.clear();
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    // Make sure default values are not invalid.
    assertThat(dnConf.getVolumeIOFailureTolerance())
        .isLessThan(dnConf.getVolumeIOTestCount());
  }

  @Test
  public void testCheckIOInvalidConfig() {
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);

    // When failure tolerance is above test count, default values should be
    // used.
    dnConf.setVolumeIOTestCount(3);
    dnConf.setVolumeIOFailureTolerance(4);
    CONF.setFromObject(dnConf);
    dnConf = CONF.getObject(DatanodeConfiguration.class);
    assertEquals(dnConf.getVolumeIOTestCount(),
        DatanodeConfiguration.DISK_CHECK_IO_TEST_COUNT_DEFAULT);
    assertEquals(dnConf.getVolumeIOFailureTolerance(),
        DatanodeConfiguration.DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT);

    // When test count and failure tolerance are set to the same value,
    // Default values should be used.
    dnConf.setVolumeIOTestCount(2);
    dnConf.setVolumeIOFailureTolerance(2);
    CONF.setFromObject(dnConf);
    dnConf = CONF.getObject(DatanodeConfiguration.class);
    assertEquals(DatanodeConfiguration.DISK_CHECK_IO_TEST_COUNT_DEFAULT,
        dnConf.getVolumeIOTestCount());
    assertEquals(DatanodeConfiguration.DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT,
        dnConf.getVolumeIOFailureTolerance());

    // Negative test count should reset to default value.
    dnConf.setVolumeIOTestCount(-1);
    CONF.setFromObject(dnConf);
    dnConf = CONF.getObject(DatanodeConfiguration .class);
    assertEquals(DatanodeConfiguration.DISK_CHECK_IO_TEST_COUNT_DEFAULT,
        dnConf.getVolumeIOTestCount());

    // Negative failure tolerance should reset to default value.
    dnConf.setVolumeIOFailureTolerance(-1);
    CONF.setFromObject(dnConf);
    dnConf = CONF.getObject(DatanodeConfiguration .class);
    assertEquals(DatanodeConfiguration.DISK_CHECK_IO_FAILURES_TOLERATED_DEFAULT,
        dnConf.getVolumeIOFailureTolerance());
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckIOInitiallyPassing(StorageVolume.Builder<?> builder)
      throws Exception {
    testCheckIOUntilFailure(builder, 3, 1, true, true, true, false, true,
        false);
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckIOEarlyFailure(StorageVolume.Builder<?> builder)
      throws Exception {
    testCheckIOUntilFailure(builder, 3, 1, false, false);
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckIOFailuresDiscarded(StorageVolume.Builder<?> builder)
      throws Exception {
    testCheckIOUntilFailure(builder, 3, 1, false, true, true, true, false,
        false);
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCheckIOAlternatingFailures(StorageVolume.Builder<?> builder)
      throws Exception {
    testCheckIOUntilFailure(builder, 3, 1, true, false, true, false);
  }

  /**
   * Helper method to test the sliding window of IO checks before volume
   * failure.
   *
   * @param ioTestCount The number of most recent tests whose results should
   *    be considered.
   * @param ioFailureTolerance The number of IO failures tolerated out of the
   *    last {@param ioTestCount} tests.
   * @param checkResults The result of the IO check for each run. Volume
   *    should fail after the last IO check is completed.
   */
  private void testCheckIOUntilFailure(StorageVolume.Builder<?> builder,
      int ioTestCount, int ioFailureTolerance, boolean... checkResults)
      throws Exception {
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    dnConf.setVolumeIOTestCount(ioTestCount);
    dnConf.setVolumeIOFailureTolerance(ioFailureTolerance);
    dnConf.setDiskCheckSlidingWindowTimeout(Duration.ofMillis(ioTestCount));
    CONF.setFromObject(dnConf);
    builder.conf(CONF);
    StorageVolume volume = builder.build();
    volume.format(CLUSTER_ID);
    volume.createTmpDirs(CLUSTER_ID);
    // Sliding window protocol transitioned from count-based to a time-based system
    // Update the default failure duration of the window from 60 minutes to a shorter duration for the test
    long eventRate = 1L;
    TestClock testClock = TestClock.newInstance();
    Field clock = SlidingWindow.class.getDeclaredField("clock");
    clock.setAccessible(true);
    clock.set(volume.getIoTestSlidingWindow(), testClock);

    for (int i = 0; i < checkResults.length; i++) {
      // Sleep to allow entries in the sliding window to eventually timeout
      testClock.fastForward(eventRate);
      final boolean result = checkResults[i];
      final DiskCheckUtil.DiskChecks ioResult = new DiskCheckUtil.DiskChecks() {
            @Override
            public boolean checkReadWrite(File storageDir, File testDir,
                int numBytesToWrite) {
              return result;
            }
          };
      DiskCheckUtil.setTestImpl(ioResult);
      if (i < checkResults.length - 1) {
        assertEquals(VolumeCheckResult.HEALTHY, volume.check(false),
            "Unexpected IO failure in run " + i);
      } else {
        assertEquals(VolumeCheckResult.FAILED, volume.check(false),
            "Unexpected IO success in run " + i);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("volumeBuilders")
  public void testCorrectDirectoryChecked(StorageVolume.Builder<?> builder)
      throws Exception {
    StorageVolume volume = builder.build();
    DiskCheckUtil.setTestImpl(new DirectoryCheck(volume));
    volume.format(CLUSTER_ID);
    volume.createTmpDirs(CLUSTER_ID);
    volume.check(false);
  }

  /**
   * Asserts that the disk checks are being done on the correct directory for
   * each volume type.
   */
  private static final class DirectoryCheck implements
      DiskCheckUtil.DiskChecks {
    private final StorageVolume volume;

    DirectoryCheck(StorageVolume volume) {
      this.volume = volume;
    }

    @Override
    public boolean checkExistence(File storageDir) {
      assertEquals(volume.getStorageDir(), storageDir);
      return true;
    }

    @Override
    public boolean checkPermissions(File storageDir) {
      assertEquals(volume.getStorageDir(), storageDir);
      return true;
    }

    @Override
    public boolean checkReadWrite(File storageDir, File testFileDir,
        int numBytesToWrite) {
      assertEquals(volume.getStorageDir(), storageDir);

      Path expectedDiskCheckPath;
      if (volume instanceof MetadataVolume) {
        expectedDiskCheckPath = Paths.get(
            volume.getStorageDir().getAbsolutePath(),
            StorageVolume.TMP_DIR_NAME,
            StorageVolume.TMP_DISK_CHECK_DIR_NAME);
      } else {
        expectedDiskCheckPath = Paths.get(
            volume.getStorageDir().getAbsolutePath(),
            volume.getClusterID(),
            StorageVolume.TMP_DIR_NAME,
            StorageVolume.TMP_DISK_CHECK_DIR_NAME);
      }

      assertEquals(expectedDiskCheckPath.toFile(), volume.getDiskCheckDir());
      assertEquals(expectedDiskCheckPath.toFile(), testFileDir);
      return true;
    }
  }
}
