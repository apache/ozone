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

    final DiskCheckUtil.DiskChecks doesNotExist = new DiskCheckUtil.DiskChecks() {
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

    final DiskCheckUtil.DiskChecks noPermissions = new DiskCheckUtil.DiskChecks() {
      @Override
      public boolean checkPermissions(File storageDir) {
        return false;
      }
    };

    DiskCheckUtil.setTestImpl(noPermissions);
    result = volume.check(false);
    assertEquals(VolumeCheckResult.FAILED, result);
  }


  @Test
  public void testCheckIoFailure() throws Exception {
    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);

    assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));

    final DiskCheckUtil.DiskChecks ioFailure = new DiskCheckUtil.DiskChecks() {
      @Override
      public boolean checkReadWrite(File storageDir, File testFileDir,
                                    int numBytesToWrite) {
        return false;
      }
    };

    // Volume should not fail until it crosses the specified failure threshold.
    int numFailuresTolerated = CONF.getObject(DatanodeConfiguration.class)
        .getConsecutiveVolumeIOFailuresTolerated();

    // Trigger failures until just before the threshold.
    DiskCheckUtil.setTestImpl(ioFailure);
    for (int i = 0; i < numFailuresTolerated - 1; i++) {
      assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
    }
    // After a passing run, the failure count should reset.
    DiskCheckUtil.clearTestImpl();
    assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
    assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));

    // Trigger failures until just before the threshold.
    DiskCheckUtil.setTestImpl(ioFailure);
    for (int i = 0; i < numFailuresTolerated - 1; i++) {
      assertEquals(VolumeCheckResult.HEALTHY, volume.check(false));
    }
    // The last run should now fail.
    assertEquals(VolumeCheckResult.FAILED, volume.check(false));
  }
}
