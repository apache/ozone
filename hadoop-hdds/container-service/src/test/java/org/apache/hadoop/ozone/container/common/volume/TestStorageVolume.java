/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DiskCheckUtil;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for StorageVolume.
 */
public class TestStorageVolume {

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HddsVolume.Builder volumeBuilder;
  private File versionFile;

  private static final DiskCheckUtil.DiskChecks IO_FAILURE =
      new DiskCheckUtil.DiskChecks() {
        @Override
        public boolean checkReadWrite(File storageDir, File testFileDir,
                                      int numBytesToWrite) {
          return false;
        }
      };

  @Before
  public void setup() throws Exception {
    File rootDir = new File(folder.getRoot(), HddsVolume.HDDS_VOLUME_DIR);
    volumeBuilder = new HddsVolume.Builder(folder.getRoot().getPath())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
    versionFile = StorageVolumeUtil.getVersionFile(rootDir);
    DiskCheckUtil.clearTestImpl();
  }

  @Test
  public void testReadPropertiesFromVersionFile() throws Exception {
    HddsVolume volume = volumeBuilder.build();

    volume.format(CLUSTER_ID);

    Properties properties = DatanodeVersionFile.readFrom(versionFile);

    String storageID = StorageVolumeUtil.getStorageID(properties, versionFile);
    String clusterID = StorageVolumeUtil.getClusterID(
        properties, versionFile, CLUSTER_ID);
    String datanodeUuid = StorageVolumeUtil.getDatanodeUUID(
        properties, versionFile, DATANODE_UUID);
    long cTime = StorageVolumeUtil.getCreationTime(
        properties, versionFile);
    int layoutVersion = StorageVolumeUtil.getLayOutVersion(
        properties, versionFile);

    assertEquals(volume.getStorageID(), storageID);
    assertEquals(volume.getClusterID(), clusterID);
    assertEquals(volume.getDatanodeUuid(), datanodeUuid);
    assertEquals(volume.getCTime(), cTime);
    assertEquals(volume.getLayoutVersion(), layoutVersion);
  }

  @Test
  public void testCheckExistence() throws Exception {
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);

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

  @Test
  public void testCheckPermissions() throws Exception {
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);

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
  @Test
  public void testCheckIODisabled() throws Exception {
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    dnConf.setVolumeIOTestCount(0);
    CONF.setFromObject(dnConf);
    volumeBuilder.conf(CONF);
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);

    DiskCheckUtil.setTestImpl(IO_FAILURE);
    assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
  }

  @Test
  public void testCheckIODefaultConfigs() {
    CONF.clear();
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    // Make sure default values are not invalid.
    assertTrue(dnConf.getVolumeIOFailureTolerance() <
        dnConf.getVolumeIOTestCount());
  }

  @Test
  public void testCheckIOInvalidConfig() throws Exception {
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);
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

  @Test
  public void testCheckIOInitiallyPassing() throws Exception {
    testCheckIOUntilFailure(3, 1, true, true, true, false, true, false);
  }

  @Test
  public void testCheckIOEarlyFailure() throws Exception {
    testCheckIOUntilFailure(3, 1, false, false);
  }

  @Test
  public void testCheckIOFailuresDiscarded() throws Exception {
    testCheckIOUntilFailure(3, 1, false, true, true, true, false, false);
  }

  @Test
  public void testCheckIOAlternatingFailures() throws Exception {
    testCheckIOUntilFailure(3, 1, true, false, true, false);
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
  private void testCheckIOUntilFailure(int ioTestCount, int ioFailureTolerance,
      boolean... checkResults) throws Exception {
    DatanodeConfiguration dnConf = CONF.getObject(DatanodeConfiguration.class);
    dnConf.setVolumeIOTestCount(ioTestCount);
    dnConf.setVolumeIOFailureTolerance(ioFailureTolerance);
    CONF.setFromObject(dnConf);
    volumeBuilder.conf(CONF);
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);

    for (int i = 0; i < checkResults.length; i++) {
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
        assertEquals("Unexpected IO failure in run " + i,
            VolumeCheckResult.HEALTHY, volume.check(false));
      } else {
        assertEquals("Unexpected IO success in run " + i,
            VolumeCheckResult.FAILED, volume.check(false));
      }
    }
  }
}
