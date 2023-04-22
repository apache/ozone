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
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;

/**
 * Tests SnapshotChain that stores in chronological order
 * Ozone object storage snapshots.  There exist two types
 * of chains provided by the SnapshotChainManager
 * i.) path based snapshots - a list of snapshots taken for a given bucket
 * ii.) global snapshots - a list of every snapshot taken in chrono order
 */
public class TestSnapshotChain {
  private OMMetadataManager omMetadataManager;
  private Map<String, SnapshotInfo> sinfos;
  private SnapshotChainManager chainManager;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS,
        folder.getRoot().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(conf);
    sinfos = new HashMap<>();
    chainManager = new SnapshotChainManager(omMetadataManager);
  }

  private SnapshotInfo createSnapshotInfo(String volName,
                                          String bucketName,
                                          String snapshotName,
                                          String snapshotID,
                                          String pathPrevID,
                                          String globalPrevID) {
    return new SnapshotInfo.Builder()
        .setSnapshotID(snapshotID)
        .setName(snapshotName)
        .setVolumeName(volName)
        .setBucketName(bucketName)
        .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE)
        .setCreationTime(Time.now())
        .setDeletionTime(-1L)
        .setPathPreviousSnapshotID(pathPrevID)
        .setGlobalPreviousSnapshotID(globalPrevID)
        .setSnapshotPath(String.join("/", volName, bucketName))
        .setCheckpointDir("checkpoint.testdir")
        .build();
  }

  private void deleteSnapshot(String snapshotID) throws IOException {
    SnapshotInfo sinfo = null;
    final String snapshotPath = "vol1/bucket1";
    // reset the next snapshotInfo.globalPreviousSnapshotID
    // to the previous in the entry to be deleted.
    if (chainManager.hasNextGlobalSnapshot(snapshotID)) {
      sinfo = sinfos
              .get(chainManager.nextGlobalSnapshot(snapshotID));
      if (chainManager.hasPreviousGlobalSnapshot(snapshotID)) {
        sinfo.setGlobalPreviousSnapshotID(chainManager
                .previousGlobalSnapshot(snapshotID));
      } else {
        sinfo.setGlobalPreviousSnapshotID(null);
      }
      sinfos.put(sinfo.getSnapshotID(), sinfo);
    }
    // reset the next snapshotInfo.pathPreviousSnapshotID
    // to the previous in the entry to be deleted.
    if (chainManager.hasNextPathSnapshot(snapshotPath,
            snapshotID)) {
      sinfo = sinfos.get(chainManager
              .nextPathSnapshot(snapshotPath,
                      snapshotID));
      if (chainManager.hasPreviousPathSnapshot(snapshotPath,
              snapshotID)) {
        sinfo.setPathPreviousSnapshotID(chainManager
                .previousPathSnapshot(snapshotPath,
                        snapshotID));
      } else {
        sinfo.setPathPreviousSnapshotID(null);
      }
      sinfos.put(sinfo.getSnapshotID(), sinfo);
    }
    // append snapshot to the sinfos (the end).
    if (chainManager.getLatestGlobalSnapshot() != null) {
      sinfo = sinfos.get(snapshotID);
      sinfo.setGlobalPreviousSnapshotID(chainManager
              .getLatestGlobalSnapshot());
      sinfo.setPathPreviousSnapshotID(chainManager
              .getLatestPathSnapshot(String
                      .join("/", "vol1", "bucket1")));
      sinfos.put(snapshotID, sinfo);
    }
    chainManager.deleteSnapshot(sinfos.get(snapshotID));
  }

  @Test
  public void testAddSnapshot() throws Exception {
    final String snapshotPath = "vol1/bucket1";
    // add three snapshots
    String snapshotID1 = UUID.randomUUID().toString();
    String snapshotID2 = UUID.randomUUID().toString();
    String snapshotID3 = UUID.randomUUID().toString();

    ArrayList<String> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);
    snapshotIDs.add(snapshotID3);

    String prevSnapshotID = null;

    // add 3 snapshots
    for (String snapshotID : snapshotIDs) {
      chainManager.addSnapshot(createSnapshotInfo("vol1",
          "bucket1",
          "test",
          snapshotID,
          prevSnapshotID,
          prevSnapshotID));
      prevSnapshotID = snapshotID;
    }

    Assert.assertEquals(snapshotID3,
        chainManager
            .getLatestGlobalSnapshot());
    Assert.assertEquals(snapshotID3,
        chainManager
            .getLatestPathSnapshot(String
                .join("/", "vol1", "bucket1")));

    int i = 0;
    String curID = snapshotIDs.get(i);
    while (chainManager.hasNextGlobalSnapshot(curID)) {
      i++;
      Assert.assertEquals(snapshotIDs.get(i),
              chainManager.nextGlobalSnapshot(curID));
      curID = snapshotIDs.get(i);
    }

    curID = snapshotIDs.get(i);
    while (chainManager.hasPreviousGlobalSnapshot(curID)) {
      i--;
      Assert.assertEquals(snapshotIDs.get(i),
              chainManager.previousGlobalSnapshot(curID));
      curID = snapshotIDs.get(i);
    }

    i = 0;
    curID = snapshotIDs.get(i);
    while (chainManager.hasNextPathSnapshot(snapshotPath,
            curID)) {
      i++;
      Assert.assertEquals(snapshotIDs.get(i),
              chainManager.nextPathSnapshot(snapshotPath,
                      curID));
      curID = snapshotIDs.get(i);
    }

    curID = snapshotIDs.get(i);
    while (chainManager.hasPreviousPathSnapshot(snapshotPath,
            curID)) {
      i--;
      Assert.assertEquals(snapshotIDs.get(i),
              chainManager.previousPathSnapshot(snapshotPath,
                      curID));
      curID = snapshotIDs.get(i);
    }
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    // add three snapshots
    String snapshotID1 = UUID.randomUUID().toString();
    String snapshotID2 = UUID.randomUUID().toString();
    String snapshotID3 = UUID.randomUUID().toString();

    ArrayList<String> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);
    snapshotIDs.add(snapshotID3);

    String prevSnapshotID = null;

    // add 3 snapshots
    for (String snapshotID : snapshotIDs) {
      sinfos.put(snapshotID,
          createSnapshotInfo("vol1",
              "bucket1",
              "test",
              snapshotID,
              prevSnapshotID,
              prevSnapshotID));

      chainManager
          .addSnapshot(sinfos.get(snapshotID));
      prevSnapshotID = snapshotID;
    }

    // delete snapshotID2
    // should have snapshots ID1 and ID3 in chain

    deleteSnapshot(snapshotID2);
    // start with first snapshot in snapshot chain
    Assert.assertEquals(false, chainManager
            .hasPreviousGlobalSnapshot(snapshotID1));
    Assert.assertEquals(true, chainManager
            .hasNextGlobalSnapshot(snapshotID1));
    Assert.assertEquals(snapshotID3, chainManager
            .nextGlobalSnapshot(snapshotID1));
    Assert.assertEquals(false, chainManager
            .hasNextGlobalSnapshot(snapshotID3));

    // add snapshotID2 and delete snapshotID1
    // should have snapshotID3 and snapshotID2
    deleteSnapshot(snapshotID1);
    chainManager
        .addSnapshot(sinfos.get(snapshotID2));

    Assert.assertEquals(false,
            chainManager.hasPreviousGlobalSnapshot(snapshotID3));
    Assert.assertEquals(true,
            chainManager.hasNextGlobalSnapshot(snapshotID3));
    Assert.assertEquals(snapshotID2,
            chainManager.nextGlobalSnapshot(snapshotID3));
    Assert.assertEquals(false,
            chainManager.hasNextGlobalSnapshot(snapshotID2));
    Assert.assertEquals(snapshotID3, chainManager
            .previousGlobalSnapshot(snapshotID2));
  }

  @Test
  public void testChainFromLoadFromTable() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
            omMetadataManager.getSnapshotInfoTable();

    // add two snapshots to the snapshotInfo
    String snapshotID1 = UUID.randomUUID().toString();
    String snapshotID2 = UUID.randomUUID().toString();

    ArrayList<String> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);

    String prevSnapshotID = "";

    // add 3 snapshots
    for (String snapshotID : snapshotIDs) {
      snapshotInfo.put(snapshotID,
              createSnapshotInfo("vol1",
                      "bucket1",
                      "test",
                      snapshotID,
                      prevSnapshotID,
                      prevSnapshotID));

      prevSnapshotID = snapshotID;
    }

    chainManager.loadSnapshotInfo(omMetadataManager);
    // check if snapshots loaded correctly from snapshotInfoTable
    Assert.assertEquals(snapshotID2, chainManager.getLatestGlobalSnapshot());
    Assert.assertEquals(snapshotID2, chainManager
            .nextGlobalSnapshot(snapshotID1));
    Assert.assertEquals(snapshotID1, chainManager.previousPathSnapshot(String
            .join("/", "vol1", "bucket1"), snapshotID2));
    Assert.assertThrows(NoSuchElementException.class,
            () -> chainManager.nextGlobalSnapshot(snapshotID2));
    Assert.assertThrows(NoSuchElementException.class,
            () -> chainManager.previousPathSnapshot(String
                    .join("/", "vol1", "bucket1"), snapshotID1));
  }
}
