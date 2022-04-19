/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * A util class for {@link HddsVolumeUtil}.
 */
public class TestHddsVolumeUtil {
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private final String datanodeId = UUID.randomUUID().toString();
  private final String clusterId = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private static final int VOLUMNE_NUM = 3;
  private MutableVolumeSet hddsVolumeSet;
  private MutableVolumeSet dbVolumeSet;

  @Before
  public void setup() throws Exception {
    // Create hdds volumes for loadAll test.
    File[] hddsVolumeDirs = new File[VOLUMNE_NUM];
    StringBuilder hddsDirs = new StringBuilder();
    for (int i = 0; i < VOLUMNE_NUM; i++) {
      hddsVolumeDirs[i] = tempDir.newFolder();
      hddsDirs.append(hddsVolumeDirs[i]).append(",");
    }
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, hddsDirs.toString());
    hddsVolumeSet = new MutableVolumeSet(datanodeId, clusterId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    // Create db volumes for format and loadAll test.
    File[] dbVolumeDirs = new File[VOLUMNE_NUM];
    StringBuilder dbDirs = new StringBuilder();
    for (int i = 0; i < VOLUMNE_NUM; i++) {
      dbVolumeDirs[i] = tempDir.newFolder();
      dbDirs.append(dbVolumeDirs[i]).append(",");
    }
    conf.set(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        dbDirs.toString());
    dbVolumeSet = new MutableVolumeSet(datanodeId, clusterId, conf, null,
        StorageVolume.VolumeType.DB_VOLUME, null);
  }

  @After
  public void teardown() {
    hddsVolumeSet.shutdown();
    dbVolumeSet.shutdown();
  }

  @Test
  public void testLoadAllHddsVolumeDbStoreWithoutDbVolumes()
      throws IOException {
    // Create db instances for all HddsVolumes.
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      hddsVolume.format(clusterId);
      hddsVolume.createWorkingDir(clusterId, null);
    }

    // Reinitialize all the volumes to simulate a DN restart.
    reinitVolumes();
    HddsVolumeUtil.loadAllHddsVolumeDbStore(hddsVolumeSet, null, null);

    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File storageIdDir = new File(new File(hddsVolume.getStorageDir(),
          clusterId), hddsVolume.getStorageID());

      // No dbVolumes given, so use the hddsVolume to store db instance.
      assertNull(hddsVolume.getDbVolume());
      assertEquals(storageIdDir, hddsVolume.getDbParentDir());
    }
  }

  @Test
  public void testLoadAllHddsVolumeDbStoreWithDbVolumes()
      throws IOException {
    // Initialize all DbVolumes
    for (DbVolume dbVolume : StorageVolumeUtil.getDbVolumesList(
        dbVolumeSet.getVolumesList())) {
      dbVolume.format(clusterId);
      dbVolume.createWorkingDir(clusterId, null);
    }

    // Create db instances for all HddsVolumes.
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      hddsVolume.format(clusterId);
      hddsVolume.createWorkingDir(clusterId, dbVolumeSet);
    }

    // Reinitialize all the volumes to simulate a DN restart.
    reinitVolumes();
    HddsVolumeUtil.loadAllHddsVolumeDbStore(hddsVolumeSet, dbVolumeSet, null);

    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File storageIdDir = new File(new File(hddsVolume.getStorageDir(),
          clusterId), hddsVolume.getStorageID());

      // Should not use the hddsVolume itself
      assertNotNull(hddsVolume.getDbVolume());
      assertNotNull(hddsVolume.getDbParentDir());
      assertNotEquals(storageIdDir, hddsVolume.getDbParentDir());
    }
  }

  private void reinitVolumes() throws IOException {
    hddsVolumeSet.shutdown();
    dbVolumeSet.shutdown();

    hddsVolumeSet = new MutableVolumeSet(datanodeId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    dbVolumeSet = new MutableVolumeSet(datanodeId, conf, null,
        StorageVolume.VolumeType.DB_VOLUME, null);
  }
}
