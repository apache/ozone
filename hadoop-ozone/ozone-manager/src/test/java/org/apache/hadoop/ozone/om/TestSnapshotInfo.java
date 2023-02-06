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
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests SnapshotInfo om database table for Ozone object storage snapshots.
 */
public class TestSnapshotInfo {

  private OMMetadataManager omMetadataManager;
  private static final String EXPECTED_SNAPSHOT_KEY = "snapshot1";
  private static final String EXPECTED_SNAPSHOT_ID =
      UUID.randomUUID().toString();
  private static final String EXPECTED_PREVIOUS_SNAPSHOT_ID =
      UUID.randomUUID().toString();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.getRoot().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf);
  }

  private SnapshotInfo createSnapshotInfo() {
    return new SnapshotInfo.Builder()
        .setSnapshotID(EXPECTED_SNAPSHOT_ID)
        .setName("snapshot1")
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setSnapshotStatus(SnapshotStatus.SNAPSHOT_ACTIVE)
        .setCreationTime(Time.now())
        .setDeletionTime(-1L)
        .setPathPreviousSnapshotID(EXPECTED_PREVIOUS_SNAPSHOT_ID)
        .setGlobalPreviousSnapshotID(EXPECTED_PREVIOUS_SNAPSHOT_ID)
        .setSnapshotPath("test/path")
        .setCheckpointDir("checkpoint.testdir")
        .build();
  }

  @Test
  public void testTableExists() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    Assert.assertTrue(snapshotInfo.isEmpty());
  }

  @Test
  public void testAddNewSnapshot() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    Assert.assertEquals(EXPECTED_SNAPSHOT_ID,
        snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getSnapshotID());
  }

  @Test
  public void testDeleteSnapshotInfo() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();

    Assert.assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    Assert.assertTrue(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.delete(EXPECTED_SNAPSHOT_KEY);
    Assert.assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
  }
}
