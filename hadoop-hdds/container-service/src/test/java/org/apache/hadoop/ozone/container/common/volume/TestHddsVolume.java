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

import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;

import static org.apache.hadoop.hdds.fs.MockSpaceUsagePersistence.inMemory;
import static org.apache.hadoop.hdds.fs.MockSpaceUsageSource.fixed;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link HddsVolume}.
 */
public class TestHddsVolume {

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static final String RESERVED_SPACE = "100B";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HddsVolume.Builder volumeBuilder;
  private File versionFile;

  @Before
  public void setup() throws Exception {
    File rootDir = new File(folder.getRoot(), HddsVolume.HDDS_VOLUME_DIR);
    CONF.set(ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED, folder.getRoot() +
        ":" + RESERVED_SPACE);
    volumeBuilder = new HddsVolume.Builder(folder.getRoot().getPath())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
    versionFile = HddsVolumeUtil.getVersionFile(rootDir);
  }

  @Test
  public void testHddsVolumeInitialization() throws Exception {
    HddsVolume volume = volumeBuilder.build();

    // The initial state of HddsVolume should be "NOT_FORMATTED" when
    // clusterID is not specified and the version file should not be written
    // to disk.
    assertNull(volume.getClusterID());
    assertEquals(StorageType.DEFAULT, volume.getStorageType());
    assertEquals(HddsVolume.VolumeState.NOT_FORMATTED,
        volume.getStorageState());
    assertFalse("Version file should not be created when clusterID is not " +
        "known.", versionFile.exists());


    // Format the volume with clusterID.
    volume.format(CLUSTER_ID);

    // The state of HddsVolume after formatting with clusterID should be
    // NORMAL and the version file should exist.
    assertTrue("Volume format should create Version file",
        versionFile.exists());
    assertEquals(CLUSTER_ID, volume.getClusterID());
    assertEquals(HddsVolume.VolumeState.NORMAL, volume.getStorageState());
  }

  @Test
  public void testReadPropertiesFromVersionFile() throws Exception {
    HddsVolume volume = volumeBuilder.build();

    volume.format(CLUSTER_ID);

    Properties properties = DatanodeVersionFile.readFrom(versionFile);

    String storageID = HddsVolumeUtil.getStorageID(properties, versionFile);
    String clusterID = HddsVolumeUtil.getClusterID(
        properties, versionFile, CLUSTER_ID);
    String datanodeUuid = HddsVolumeUtil.getDatanodeUUID(
        properties, versionFile, DATANODE_UUID);
    long cTime = HddsVolumeUtil.getCreationTime(
        properties, versionFile);
    int layoutVersion = HddsVolumeUtil.getLayOutVersion(
        properties, versionFile);

    assertEquals(volume.getStorageID(), storageID);
    assertEquals(volume.getClusterID(), clusterID);
    assertEquals(volume.getDatanodeUuid(), datanodeUuid);
    assertEquals(volume.getCTime(), cTime);
    assertEquals(volume.getLayoutVersion(), layoutVersion);
  }

  @Test
  public void testShutdown() throws Exception {
    long initialUsedSpace = 250;
    AtomicLong savedUsedSpace = new AtomicLong(initialUsedSpace);
    SpaceUsagePersistence persistence = inMemory(savedUsedSpace);
    SpaceUsageSource spaceUsage = fixed(500, 200);
    long expectedUsedSpace = spaceUsage.getUsedSpace();
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, persistence);
    volumeBuilder.usageCheckFactory(factory);

    HddsVolume volume = volumeBuilder.build();

    assertEquals(initialUsedSpace, savedUsedSpace.get());
    assertEquals(expectedUsedSpace, volume.getUsedSpace());

    // Shutdown the volume.
    volume.shutdown();

    // Volume state should be "NON_EXISTENT" when volume is shutdown.
    assertEquals(HddsVolume.VolumeState.NON_EXISTENT, volume.getStorageState());

    // Volume should save scmUsed cache file once volume is shutdown
    assertEquals(expectedUsedSpace, savedUsedSpace.get());

    // Volume.getAvailable() should succeed even when usage thread
    // is shutdown.
    StorageSize size = StorageSize.parse(RESERVED_SPACE);
    long reservedSpaceInBytes = (long) size.getUnit().toBytes(size.getValue());

    assertEquals(spaceUsage.getCapacity(),
        volume.getCapacity() + reservedSpaceInBytes);
    assertEquals(spaceUsage.getAvailable(),
        volume.getAvailable() + reservedSpaceInBytes);
  }

}
