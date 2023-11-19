/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests SnapshotInfo om database table for Ozone object storage snapshots.
 */
public class TestSnapshotInfo {

  private OMMetadataManager omMetadataManager;
  private static final String EXPECTED_SNAPSHOT_KEY = "snapshot1";
  private static final UUID EXPECTED_SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID EXPECTED_PREVIOUS_SNAPSHOT_ID = UUID.randomUUID();

  @TempDir
  private Path folder;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
  }

  private SnapshotInfo createSnapshotInfo() {
    return new SnapshotInfo.Builder()
        .setSnapshotId(EXPECTED_SNAPSHOT_ID)
        .setName("snapshot1")
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setSnapshotStatus(SnapshotStatus.SNAPSHOT_ACTIVE)
        .setCreationTime(Time.now())
        .setDeletionTime(-1L)
        .setPathPreviousSnapshotId(EXPECTED_PREVIOUS_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotId(EXPECTED_PREVIOUS_SNAPSHOT_ID)
        .setSnapshotPath("test/path")
        .setCheckpointDir("checkpoint.testdir")
        .build();
  }

  @Test
  public void testTableExists() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    Assertions.assertTrue(snapshotInfo.isEmpty());
  }

  @Test
  public void testAddNewSnapshot() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    Assertions.assertEquals(EXPECTED_SNAPSHOT_ID,
        snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getSnapshotId());
  }

  @Test
  public void testDeleteSnapshotInfo() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();

    Assertions.assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    Assertions.assertTrue(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.delete(EXPECTED_SNAPSHOT_KEY);
    Assertions.assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
  }

  @Test
  public void testSnapshotSSTFilteredFlag() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    SnapshotInfo info  = createSnapshotInfo();
    info.setSstFiltered(false);
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    Assertions.assertFalse(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY)
        .isSstFiltered());
    info.setSstFiltered(true);
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    Assertions.assertTrue(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY)
        .isSstFiltered());
  }
}
