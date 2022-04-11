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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A util class for {@link HddsVolumeUtil}.
 */
public class TestHddsVolumeUtil {
  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  private final String datanodeId = UUID.randomUUID().toString();
  private final String clusterId = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private static final int VOLUMNE_NUM = 5;
  private MutableVolumeSet hddsVolumeSet;
  private MutableVolumeSet dbVolumeSet;
  private HddsVolume singleHddsVolume;

  @Before
  public void setup() throws Exception {
    // Create a single volume for format test.
    File hddsVolumeDir = tempDir.newFolder();
    singleHddsVolume = new HddsVolume.Builder(hddsVolumeDir
        .getAbsolutePath()).conf(conf).datanodeUuid(datanodeId)
        .clusterID(clusterId).build();

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

  @Test
  public void testFormatDbStoreForHddsVolumeWithoutDbVolumes()
      throws IOException {
    HddsVolumeUtil.formatDbStoreForHddsVolume(singleHddsVolume,
        null, clusterId, conf);

    File storageIdDir = new File(new File(singleHddsVolume.getStorageDir(),
        clusterId), singleHddsVolume.getStorageID());

    // No dbVolumes given, so use the hddsVolume to store db instance.
    assertNull(singleHddsVolume.getDbVolume());
    assertEquals(storageIdDir, singleHddsVolume.getDbParentDir());

    // The db directory should exist.
    File containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME);
    assertTrue(containerDBPath.exists());
  }

  @Test
  public void testFormatDbStoreForHddsVolumeWithDbVolumes()
      throws IOException {
    HddsVolumeUtil.formatDbStoreForHddsVolume(singleHddsVolume,
        dbVolumeSet, clusterId, conf);

    // A dbVolume is picked.
    assertNotNull(singleHddsVolume.getDbVolume());

    File storageIdDir = new File(new File(singleHddsVolume.getDbVolume()
        .getStorageDir(), clusterId), singleHddsVolume.getStorageID());
    // Db parent dir should be set to a subdir under the dbVolume.
    assertEquals(singleHddsVolume.getDbParentDir(), storageIdDir);

    // The db directory should exist.
    File containerDBPath = new File(storageIdDir, CONTAINER_DB_NAME);
    assertTrue(containerDBPath.exists());
  }

  @Test
  public void testLoadAllHddsVolumeDbStoreWithoutDbVolumes()
      throws IOException {
    // Create storageID subdirs under each hddsVolume manually
    // instead of format a real db instance.
    for (HddsVolume hddsVolume : StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList())) {
      File storageIdDir = new File(new File(hddsVolume.getStorageDir(),
          clusterId), hddsVolume.getStorageID());
      if (!storageIdDir.mkdirs()) {
        throw new IOException("Unexpected mkdir failed");
      }
    }

    // Reinitialize all the volumes to simulate a DN restart.
    reinitVolumes();
    HddsVolumeUtil.loadAllHddsVolumeDbStore(hddsVolumeSet, null,
        conf, null);

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
    // Create storageID subdirs under dbVolumes manually
    // instead of format a real db instance.
    // Map hddsVolume[i] -> dbVolume[i]
    Iterator<HddsVolume> hddsVolumeIter = StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList()).iterator();
    Iterator<DbVolume> dbVolumeIter = StorageVolumeUtil.getDbVolumesList(
        dbVolumeSet.getVolumesList()).iterator();
    for (int i = 0; i < VOLUMNE_NUM; i++) {
      HddsVolume hddsVolume = hddsVolumeIter.next();
      DbVolume dbVolume = dbVolumeIter.next();
      File storageIdDir = new File(new File(dbVolume.getStorageDir(),
          clusterId), hddsVolume.getStorageID());
      if (!storageIdDir.mkdirs()) {
        throw new IOException("Unexpected mkdir failed");
      }
    }

    // Reinitialize all the volumes to simulate a DN restart.
    reinitVolumes();
    HddsVolumeUtil.loadAllHddsVolumeDbStore(hddsVolumeSet, dbVolumeSet,
        conf, null);

    hddsVolumeIter = StorageVolumeUtil.getHddsVolumesList(
        hddsVolumeSet.getVolumesList()).iterator();
    dbVolumeIter = StorageVolumeUtil.getDbVolumesList(
        dbVolumeSet.getVolumesList()).iterator();
    for (int i = 0; i < VOLUMNE_NUM; i++) {
      HddsVolume hddsVolume = hddsVolumeIter.next();
      DbVolume dbVolume = dbVolumeIter.next();

      // Check volume mapped correctly.
      assertEquals(dbVolume, hddsVolume.getDbVolume());

      File storageIdDir = new File(new File(dbVolume.getStorageDir(),
          clusterId), hddsVolume.getStorageID());
      // Db parent dir should be set to a subdir under the dbVolume.
      assertEquals(hddsVolume.getDbParentDir(), storageIdDir);
    }
  }

  private void reinitVolumes() throws IOException {
    // On DN startup the clusterID is null.
    hddsVolumeSet = new MutableVolumeSet(datanodeId, null, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    dbVolumeSet = new MutableVolumeSet(datanodeId, null, conf, null,
        StorageVolume.VolumeType.DB_VOLUME, null);
  }
}
