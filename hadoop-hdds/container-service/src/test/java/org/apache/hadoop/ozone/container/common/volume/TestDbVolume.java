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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link DbVolume}.
 */
public class TestDbVolume {

  private static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final String CLUSTER_ID = UUID.randomUUID().toString();
  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  private DbVolume.Builder volumeBuilder;
  private File versionFile;

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws Exception {
    File rootDir = new File(folder.toFile(), DbVolume.DB_VOLUME_DIR);
    volumeBuilder = new DbVolume.Builder(folder.toString())
        .datanodeUuid(DATANODE_UUID)
        .conf(CONF)
        .usageCheckFactory(MockSpaceUsageCheckFactory.NONE);
    versionFile = StorageVolumeUtil.getVersionFile(rootDir);
  }

  @Test
  public void testInitializeEmptyDbVolume() throws IOException {
    DbVolume volume = volumeBuilder.build();

    // The initial state of HddsVolume should be "NOT_FORMATTED" when
    // clusterID is not specified and the version file should not be written
    // to disk.
    assertNull(volume.getClusterID());
    assertEquals(StorageType.DEFAULT, volume.getStorageType());
    assertEquals(HddsVolume.VolumeState.NOT_FORMATTED,
        volume.getStorageState());
    assertFalse(versionFile.exists(), "Version file should not be created " +
        "when clusterID is not known.");

    // Format the volume with clusterID.
    volume.format(CLUSTER_ID);

    // The state of HddsVolume after formatting with clusterID should be
    // NORMAL and the version file should exist.
    assertTrue(versionFile.exists(),
        "Volume format should create Version file");
    assertEquals(CLUSTER_ID, volume.getClusterID());
    assertEquals(HddsVolume.VolumeState.NORMAL, volume.getStorageState());
    assertEquals(0, volume.getHddsVolumeIDs().size());
  }

  @Test
  public void testInitializeNonEmptyDbVolume() throws IOException {
    DbVolume volume = volumeBuilder.build();

    // The initial state of HddsVolume should be "NOT_FORMATTED" when
    // clusterID is not specified and the version file should not be written
    // to disk.
    assertNull(volume.getClusterID());
    assertEquals(StorageType.DEFAULT, volume.getStorageType());
    assertEquals(HddsVolume.VolumeState.NOT_FORMATTED,
        volume.getStorageState());
    assertFalse(versionFile.exists(), "Version file should not be created " +
        "when clusterID is not known.");

    // Format the volume with clusterID.
    volume.format(CLUSTER_ID);
    volume.createWorkingDir(CLUSTER_ID, null);

    // The clusterIdDir should be created
    File clusterIdDir = new File(volume.getStorageDir(), CLUSTER_ID);
    assertTrue(clusterIdDir.exists());

    // Create some subdirectories to mock db instances under this volume.
    int numSubDirs = 5;
    File[] subdirs = new File[numSubDirs];
    for (int i = 0; i < numSubDirs; i++) {
      subdirs[i] = new File(clusterIdDir, UUID.randomUUID().toString());
      boolean res = subdirs[i].mkdir();
      assertTrue(res);
    }

    // Rebuild the same volume to simulate DN restart.
    volume = volumeBuilder.build();
    assertEquals(numSubDirs, volume.getHddsVolumeIDs().size());
  }

  @Test
  public void testDbStoreClosedOnBadDbVolume() throws IOException {
    ContainerTestUtils.enableSchemaV3(CONF);

    DbVolume dbVolume = volumeBuilder.build();
    dbVolume.format(CLUSTER_ID);
    dbVolume.createWorkingDir(CLUSTER_ID, null);

    MutableVolumeSet dbVolumeSet = mock(MutableVolumeSet.class);
    when(dbVolumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(dbVolume));

    MutableVolumeSet hddsVolumeSet = createHddsVolumeSet(3);
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      hddsVolume.format(CLUSTER_ID);
      hddsVolume.createWorkingDir(CLUSTER_ID, dbVolumeSet);
    }

    // The db handlers should be in the cache
    assertEquals(3, DatanodeStoreCache.getInstance().size());

    // Make the dbVolume a bad volume
    dbVolume.failVolume();

    // The db handlers should be removed from the cache
    assertEquals(0, DatanodeStoreCache.getInstance().size());
  }

  @Test
  public void testFailVolumeWhileDbStorePathIsAdded() throws IOException {
    // Use a separate conf to keep the shared CONF unchanged for other tests.
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        Files.createDirectory(folder.resolve("dbDir")).toString());
    MutableVolumeSet dbVolumeSet = new MutableVolumeSet(DATANODE_UUID,
        CLUSTER_ID, conf, null, StorageVolume.VolumeType.DB_VOLUME, null);
    DbVolume dbVolume = (DbVolume) dbVolumeSet.getVolumesList().get(0);
    // The db stores are closed only if the clusterID dir exists.
    dbVolume.format(CLUSTER_ID);
    dbVolume.createWorkingDir(CLUSTER_ID, null);

    // More than one path, so that iteration continues after the new path is added.
    Set<String> dbPaths = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      String dbPath = "db" + i;
      dbVolume.addHddsDbStorePath(UUID.randomUUID().toString(), dbPath);
      dbPaths.add(dbPath);
    }

    // Register one more db store path when the first db is closed,
    // while closing the first DB, add a new path as HddsVolume#createDbStore does during SchemaV3
    // finalization while holding a different lock.
    Set<String> closedDbPaths = new HashSet<>();
    DatanodeStoreCache cache = mock(DatanodeStoreCache.class);
    doAnswer(invocation -> {
      if (closedDbPaths.isEmpty()) {
        dbVolume.addHddsDbStorePath(UUID.randomUUID().toString(), "newDb");
      }
      closedDbPaths.add(invocation.getArgument(0));
      return null;
    }).when(cache).removeDB(anyString());

    // Fail the volume the way the disk checker does
    try (MockedStatic<DatanodeStoreCache> mockedCache = mockStatic(DatanodeStoreCache.class)) {
      mockedCache.when(DatanodeStoreCache::getInstance).thenReturn(cache);
      dbVolumeSet.failVolume(dbVolume.getStorageDir().getPath());
    }

    assertEquals(HddsVolume.VolumeState.FAILED, dbVolume.getStorageState());
    // The newly added path may or may not be visited by the iteration.
    assertTrue(closedDbPaths.containsAll(dbPaths));
    // The failed volume must be moved out of the active volumes.
    assertFalse(dbVolumeSet.getVolumesList().contains(dbVolume));
    assertEquals(Collections.singletonList(dbVolume), dbVolumeSet.getFailedVolumesList());
  }

  private MutableVolumeSet createHddsVolumeSet(int volumeNum)
      throws IOException {
    File[] hddsVolumeDirs = new File[volumeNum];
    StringBuilder hddsDirs = new StringBuilder();
    for (int i = 0; i < volumeNum; i++) {
      hddsVolumeDirs[i] =
          Files.createDirectory(folder.resolve("volumeDir" + i)).toFile();
      hddsDirs.append(hddsVolumeDirs[i]).append(',');
    }
    CONF.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, hddsDirs.toString());
    MutableVolumeSet hddsVolumeSet = new MutableVolumeSet(DATANODE_UUID,
        CLUSTER_ID, CONF, null, StorageVolume.VolumeType.DATA_VOLUME, null);
    return hddsVolumeSet;
  }
}
