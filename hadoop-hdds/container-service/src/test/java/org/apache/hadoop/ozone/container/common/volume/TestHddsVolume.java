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
import java.io.IOException;
import java.time.Duration;
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
import org.apache.hadoop.ozone.OzoneConfigKeys;

import static org.apache.hadoop.hdds.fs.MockSpaceUsagePersistence.inMemory;
import static org.apache.hadoop.hdds.fs.MockSpaceUsageSource.fixed;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
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
    versionFile = StorageVolumeUtil.getVersionFile(rootDir);
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

  /**
   * Test conservative avail space.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   * |<-     capacity         ->|
   *              |     fsAvail      |-------other-------|
   *                          ->|~~~~|<-
   *                       remainingReserved
   * |<-                   fsCapacity                  ->|
   * A) avail = capacity - used
   * B) avail = fsAvail - Max(reserved - other, 0);
   *
   * So, conservatively, avail = Max(Min(A, B), 0);
   * This test avail == A, which implies there are deletes
   * that release space, but 'du' report is delayed.
   */
  @Test
  public void testReportUsedBiggerThanActualUsed() throws IOException {
    // fsCapacity = 500, fsAvail = 290, reserved = 100
    // used = 300(cached value from previous refresh)
    // actual used = 200(due to deletes)
    // so, other = max((500 - 290 - 300, 0) = 0
    // actual other = 10(system usage)
    // A = 500 - 100 - 300 = 100, B = 290 - max((100 - 0), 0) = 190
    SpaceUsageSource spaceUsage = fixed(500, 290, 300);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, inMemory(new AtomicLong(0)));
    volumeBuilder.usageCheckFactory(factory);

    HddsVolume volume = volumeBuilder.build();

    assertEquals(400, volume.getCapacity());
    assertEquals(100, volume.getAvailable());

    // Shutdown the volume.
    volume.shutdown();
  }

  /**
   * Continue the above.
   * This test avail == B, which implies there are new allocates
   * that consumes space, but 'du' report is delayed.
   */
  @Test
  public void testReportUsedSmallerThanActualUsed() throws IOException {
    // fsCapacity = 500, fsAvail = 190, reserved = 100
    // used = 0(cached value from previous refresh)
    // actual used = 300(new allocates)
    // so, other = max(500 - 190 - 0, 0) = 310
    // actual other = 10(system usage)
    // A = 500 - 100 - 0 = 400, B = 190 - max((100 - 310), 0) = 190
    SpaceUsageSource spaceUsage = fixed(500, 190, 0);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, inMemory(new AtomicLong(0)));
    volumeBuilder.usageCheckFactory(factory);

    HddsVolume volume = volumeBuilder.build();

    assertEquals(400, volume.getCapacity());
    assertEquals(190, volume.getAvailable());

    // Shutdown the volume.
    volume.shutdown();
  }

  /**
   * Test over used space by other app: other > reserved.
   * remainingReserved == 0 && avail == fsAvail
   */
  @Test
  public void testOverUsedReservedSpace() throws IOException {
    // fsCapacity = 500, fsAvail = 300, reserved = 100
    // used = 0(cached value from previous refresh)
    // actual used = 0(no writes yet)
    // so, other = max(500 - 300 - 0, 0) = 200
    // actual other = 200(e.g. yarn usage + system usage)
    // A = 500 - 100 - 0 = 400, B = 300 - max((100 - 200), 0) = 300
    SpaceUsageSource spaceUsage = fixed(500, 300, 0);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, inMemory(new AtomicLong(0)));
    volumeBuilder.usageCheckFactory(factory);

    HddsVolume volume = volumeBuilder.build();

    assertEquals(400, volume.getCapacity());
    assertEquals(300, volume.getAvailable());

    // Shutdown the volume.
    volume.shutdown();
  }

  /**
   * Test over used space by hdds.
   * used >= capacity && avail == 0
   */
  @Test
  public void testOverUsedHddsSpace() throws IOException {
    // fsCapacity = 500, fsAvail = 40, reserved = 100
    // used = 450(exact report)
    // actual used = 450
    // so, other = max(500 - 40 - 450, 0) = 10
    // actual other = 10(e.g. system usage)
    // A = 500 - 100 - 450 = -50, B = 40 - max((100 - 10), 0) = -50
    SpaceUsageSource spaceUsage = fixed(500, 40, 450);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        spaceUsage, Duration.ZERO, inMemory(new AtomicLong(0)));
    volumeBuilder.usageCheckFactory(factory);

    HddsVolume volume = volumeBuilder.build();

    assertEquals(400, volume.getCapacity());
    assertEquals(0, volume.getAvailable());

    // Shutdown the volume.
    volume.shutdown();
  }

  @Test
  public void testDbStoreCreatedWithoutDbVolumes() throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);
    volume.createWorkingDir(CLUSTER_ID, null);

    // No DbVolume chosen and use the HddsVolume itself to hold
    // a db instance.
    assertNull(volume.getDbVolume());
    File storageIdDir = new File(new File(volume.getStorageDir(),
        CLUSTER_ID), volume.getStorageID());
    assertEquals(volume.getDbParentDir(), storageIdDir);

    // The db directory should exist
    File containerDBFile = new File(volume.getDbParentDir(),
        CONTAINER_DB_NAME);
    assertTrue(containerDBFile.exists());

    volume.shutdown();
  }

  @Test
  public void testDbStoreCreatedWithDbVolumes() throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    // create the DbVolumeSet
    MutableVolumeSet dbVolumeSet = createDbVolumeSet();

    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);
    volume.createWorkingDir(CLUSTER_ID, dbVolumeSet);

    // DbVolume chosen.
    assertNotNull(volume.getDbVolume());

    File storageIdDir = new File(new File(volume.getDbVolume()
        .getStorageDir(), CLUSTER_ID), volume.getStorageID());
    // Db parent dir should be set to a subdir under the dbVolume.
    assertEquals(volume.getDbParentDir(), storageIdDir);

    // The db directory should exist
    File containerDBFile = new File(volume.getDbParentDir(),
        CONTAINER_DB_NAME);
    assertTrue(containerDBFile.exists());

    volume.shutdown();
  }

  @Test
  public void testDbStoreClosedOnBadVolumeWithoutDbVolumes()
      throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);
    volume.createWorkingDir(CLUSTER_ID, null);

    // No DbVolume chosen and use the HddsVolume itself to hold
    // a db instance.
    assertNull(volume.getDbVolume());
    File storageIdDir = new File(new File(volume.getStorageDir(),
        CLUSTER_ID), volume.getStorageID());
    assertEquals(volume.getDbParentDir(), storageIdDir);

    // The db directory should exist
    File containerDBFile = new File(volume.getDbParentDir(),
        CONTAINER_DB_NAME);
    assertTrue(containerDBFile.exists());
    assertNotNull(DatanodeStoreCache.getInstance().getDB(
        containerDBFile.getAbsolutePath(), CONF));

    // Make it a bad volume
    volume.failVolume();

    // The db should be removed from cache
    assertEquals(0, DatanodeStoreCache.getInstance().size());
  }

  @Test
  public void testDbStoreClosedOnBadVolumeWithDbVolumes() throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    // create the DbVolumeSet
    MutableVolumeSet dbVolumeSet = createDbVolumeSet();

    HddsVolume volume = volumeBuilder.build();
    volume.format(CLUSTER_ID);
    volume.createWorkingDir(CLUSTER_ID, dbVolumeSet);

    // DbVolume chosen.
    assertNotNull(volume.getDbVolume());

    File storageIdDir = new File(new File(volume.getDbVolume()
        .getStorageDir(), CLUSTER_ID), volume.getStorageID());
    // Db parent dir should be set to a subdir under the dbVolume.
    assertEquals(volume.getDbParentDir(), storageIdDir);

    // The db directory should exist
    File containerDBFile = new File(volume.getDbParentDir(),
        CONTAINER_DB_NAME);
    assertTrue(containerDBFile.exists());
    assertNotNull(DatanodeStoreCache.getInstance().getDB(
        containerDBFile.getAbsolutePath(), CONF));

    // Make it a bad volume
    volume.failVolume();

    // The db should be removed from cache
    assertEquals(0, DatanodeStoreCache.getInstance().size());
  }

  private MutableVolumeSet createDbVolumeSet() throws IOException {
    File dbVolumeDir = folder.newFolder();
    CONF.set(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        dbVolumeDir.getAbsolutePath());
    MutableVolumeSet dbVolumeSet = new MutableVolumeSet(DATANODE_UUID,
        CLUSTER_ID, CONF, null, StorageVolume.VolumeType.DB_VOLUME,
        null);
    dbVolumeSet.getVolumesList().get(0).format(CLUSTER_ID);
    dbVolumeSet.getVolumesList().get(0).createWorkingDir(CLUSTER_ID, null);
    return dbVolumeSet;
  }
}
