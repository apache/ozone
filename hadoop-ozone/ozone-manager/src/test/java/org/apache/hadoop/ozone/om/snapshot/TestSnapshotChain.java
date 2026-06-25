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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests SnapshotChain that stores in chronological order
 * Ozone object storage snapshots.  There exist two types
 * of chains provided by the SnapshotChainManager
 * i.) path based snapshots - a list of snapshots taken for a given bucket
 * ii.) global snapshots - a list of every snapshot taken in chrono order
 */
public class TestSnapshotChain {
  private OMMetadataManager omMetadataManager;
  private Map<UUID, SnapshotInfo> snapshotIdToSnapshotInfoMap;
  private SnapshotChainManager chainManager;

  @TempDir
  private File folder;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, folder.toString());
    omMetadataManager = new OmMetadataManagerImpl(conf, null);
    snapshotIdToSnapshotInfoMap = new HashMap<>();
    chainManager = new SnapshotChainManager(omMetadataManager);
  }

  private SnapshotInfo createSnapshotInfo(UUID snapshotID,
                                          UUID pathPrevID,
                                          UUID globalPrevID,
                                          long creationTime) {
    return new SnapshotInfo.Builder()
        .setSnapshotId(snapshotID)
        .setName("test")
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE)
        .setCreationTime(creationTime)
        .setDeletionTime(-1L)
        .setPathPreviousSnapshotId(pathPrevID)
        .setGlobalPreviousSnapshotId(globalPrevID)
        .setSnapshotPath(String.join("/", "vol1", "bucket1"))
        .build();
  }

  private void deleteSnapshot(UUID snapshotID) throws IOException {
    SnapshotInfo sinfo = null;
    final String snapshotPath = "vol1/bucket1";
    // reset the next snapshotInfo.globalPreviousSnapshotID
    // to the previous in the entry to be deleted.
    if (chainManager.hasNextGlobalSnapshot(snapshotID)) {
      sinfo = snapshotIdToSnapshotInfoMap.get(
          chainManager.nextGlobalSnapshot(snapshotID));
      if (chainManager.hasPreviousGlobalSnapshot(snapshotID)) {
        sinfo.setGlobalPreviousSnapshotId(
            chainManager.previousGlobalSnapshot(snapshotID));
      } else {
        sinfo.setGlobalPreviousSnapshotId(null);
      }
      snapshotIdToSnapshotInfoMap.put(sinfo.getSnapshotId(), sinfo);
    }
    // reset the next snapshotInfo.pathPreviousSnapshotID
    // to the previous in the entry to be deleted.
    if (chainManager.hasNextPathSnapshot(snapshotPath, snapshotID)) {
      sinfo = snapshotIdToSnapshotInfoMap.get(
          chainManager.nextPathSnapshot(snapshotPath, snapshotID));

      if (chainManager.hasPreviousPathSnapshot(snapshotPath, snapshotID)) {
        sinfo.setPathPreviousSnapshotId(
            chainManager.previousPathSnapshot(snapshotPath, snapshotID));
      } else {
        sinfo.setPathPreviousSnapshotId(null);
      }
      snapshotIdToSnapshotInfoMap.put(sinfo.getSnapshotId(), sinfo);
    }

    UUID latestGlobalSnapshot = chainManager.getLatestGlobalSnapshotId();
    // append snapshot to the sinfos (the end).
    if (latestGlobalSnapshot != null) {
      sinfo = snapshotIdToSnapshotInfoMap.get(snapshotID);
      sinfo.setGlobalPreviousSnapshotId(latestGlobalSnapshot);
      UUID latestPathSnapshot = chainManager.getLatestPathSnapshotId(
          String.join("/", "vol1", "bucket1"));
      if (latestPathSnapshot != null) {
        sinfo.setPathPreviousSnapshotId(latestPathSnapshot);
      }
      snapshotIdToSnapshotInfoMap.put(snapshotID, sinfo);
    }
    chainManager.deleteSnapshot(snapshotIdToSnapshotInfoMap.get(snapshotID));
  }

  @Test
  public void testAddSnapshot() throws Exception {
    final String snapshotPath = "vol1/bucket1";
    // add three snapshots
    UUID snapshotID1 = UUID.randomUUID();
    UUID snapshotID2 = UUID.randomUUID();
    UUID snapshotID3 = UUID.randomUUID();

    ArrayList<UUID> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);
    snapshotIDs.add(snapshotID3);

    UUID prevSnapshotID = null;
    List<Integer> times = Arrays.asList(1, 2, 0);
    int timeIdx = 0;
    // add 3 snapshots
    for (UUID snapshotID : snapshotIDs) {
      chainManager.addSnapshot(createSnapshotInfo(
          snapshotID,
          prevSnapshotID,
          prevSnapshotID, times.get(timeIdx++)));
      prevSnapshotID = snapshotID;
    }

    assertEquals(snapshotID3, chainManager.getLatestGlobalSnapshotId());
    assertEquals(snapshotID1, chainManager.getOldestGlobalSnapshotId());
    assertEquals(snapshotID3, chainManager.getLatestPathSnapshotId(
        String.join("/", "vol1", "bucket1")));

    int i = 0;
    UUID curID = snapshotIDs.get(i);
    while (chainManager.hasNextGlobalSnapshot(curID)) {
      i++;
      assertEquals(snapshotIDs.get(i), chainManager.nextGlobalSnapshot(curID));
      curID = snapshotIDs.get(i);
    }

    curID = snapshotIDs.get(i);
    while (chainManager.hasPreviousGlobalSnapshot(curID)) {
      i--;
      assertEquals(snapshotIDs.get(i),
          chainManager.previousGlobalSnapshot(curID));
      curID = snapshotIDs.get(i);
    }

    i = 0;
    curID = snapshotIDs.get(i);
    while (chainManager.hasNextPathSnapshot(snapshotPath, curID)) {
      i++;
      assertEquals(snapshotIDs.get(i),
          chainManager.nextPathSnapshot(snapshotPath, curID));
      curID = snapshotIDs.get(i);
    }

    curID = snapshotIDs.get(i);
    while (chainManager.hasPreviousPathSnapshot(snapshotPath,
        curID)) {
      i--;
      assertEquals(snapshotIDs.get(i),
          chainManager.previousPathSnapshot(snapshotPath, curID));
      curID = snapshotIDs.get(i);
    }
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    // add three snapshots
    UUID snapshotID1 = UUID.randomUUID();
    UUID snapshotID2 = UUID.randomUUID();
    UUID snapshotID3 = UUID.randomUUID();

    ArrayList<UUID> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);
    snapshotIDs.add(snapshotID3);

    UUID prevSnapshotID = null;
    List<Integer> times = Arrays.asList(1, 2, 0);
    int timeIdx = 0;
    // add 3 snapshots
    for (UUID snapshotID : snapshotIDs) {
      snapshotIdToSnapshotInfoMap.put(snapshotID,
          createSnapshotInfo(
              snapshotID,
              prevSnapshotID,
              prevSnapshotID, times.get(timeIdx++)));

      chainManager.addSnapshot(snapshotIdToSnapshotInfoMap.get(snapshotID));
      prevSnapshotID = snapshotID;
    }

    // delete snapshotID2
    // should have snapshots ID1 and ID3 in chain

    deleteSnapshot(snapshotID2);
    // start with first snapshot in snapshot chain
    assertFalse(chainManager.hasPreviousGlobalSnapshot(snapshotID1));
    assertTrue(chainManager.hasNextGlobalSnapshot(snapshotID1));
    assertFalse(chainManager.hasNextGlobalSnapshot(snapshotID3));
    assertEquals(snapshotID3,
        chainManager.nextGlobalSnapshot(snapshotID1));

    // add snapshotID2 and delete snapshotID1
    // should have snapshotID3 and snapshotID2
    deleteSnapshot(snapshotID1);
    chainManager.addSnapshot(snapshotIdToSnapshotInfoMap.get(snapshotID2));

    assertFalse(chainManager.hasPreviousGlobalSnapshot(snapshotID3));
    assertTrue(chainManager.hasNextGlobalSnapshot(snapshotID3));
    assertEquals(snapshotID2, chainManager.nextGlobalSnapshot(snapshotID3));
    assertFalse(chainManager.hasNextGlobalSnapshot(snapshotID2));
    assertEquals(snapshotID3, chainManager.previousGlobalSnapshot(snapshotID2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testChainFromLoadFromTable(boolean increasingTIme)
      throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
            omMetadataManager.getSnapshotInfoTable();

    // add two snapshots to the snapshotInfo
    UUID snapshotID1 = UUID.randomUUID();
    UUID snapshotID2 = UUID.randomUUID();
    List<UUID> snapshotIDs = new ArrayList<>();
    snapshotIDs.add(snapshotID1);
    snapshotIDs.add(snapshotID2);
    List<SnapshotInfo> snapshotInfoList = new ArrayList<>();

    UUID prevSnapshotID = null;
    long time = System.currentTimeMillis();
    for (UUID snapshotID : snapshotIDs) {
      SnapshotInfo snapInfo = createSnapshotInfo(snapshotID, prevSnapshotID,
          prevSnapshotID, increasingTIme ? time++ : time--);
      snapshotInfo.put(snapshotID.toString(), snapInfo);
      prevSnapshotID = snapshotID;
      snapshotInfoList.add(snapInfo);
    }

    chainManager = new SnapshotChainManager(omMetadataManager);
    assertFalse(chainManager.isSnapshotChainCorrupted());
    // check if snapshots loaded correctly from snapshotInfoTable
    assertEquals(snapshotID2, chainManager.getLatestGlobalSnapshotId());
    assertEquals(snapshotID1, chainManager.getOldestGlobalSnapshotId());
    assertEquals(snapshotID2, chainManager.nextGlobalSnapshot(snapshotID1));
    assertEquals(snapshotID1, chainManager.previousPathSnapshot(String
        .join("/", "vol1", "bucket1"), snapshotID2));
    assertThrows(NoSuchElementException.class,
        () -> chainManager.nextGlobalSnapshot(snapshotID2));
    assertThrows(NoSuchElementException.class,
        () -> chainManager.previousPathSnapshot(String
            .join("/", "vol1", "bucket1"), snapshotID1));

    UUID snapshotID3 = UUID.randomUUID();
    SnapshotInfo snapshotInfo3 = createSnapshotInfo(snapshotID3, prevSnapshotID,
        prevSnapshotID, Time.now());
    // Add and delete snapshot to make sure snapshot chain is correct.
    chainManager.addSnapshot(snapshotInfo3);
    chainManager.deleteSnapshot(snapshotInfoList.get(0));
    assertEquals(snapshotID3, chainManager.getLatestGlobalSnapshotId());
    assertThrows(NoSuchElementException.class,
        () -> chainManager.nextGlobalSnapshot(snapshotID1));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 5, 10})
  public void testSnapshotChainIterator(int numberOfSnapshots) throws IOException {
    Table<String, SnapshotInfo> snapshotInfo = omMetadataManager.getSnapshotInfoTable();
    List<SnapshotInfo> snapshotInfoList = new ArrayList<>();

    UUID prevSnapshotID = null;
    long time = System.currentTimeMillis();
    for (int i = 0; i < numberOfSnapshots; i++) {
      UUID snapshotID = UUID.randomUUID();
      SnapshotInfo snapInfo = createSnapshotInfo(snapshotID, prevSnapshotID,
          prevSnapshotID, time++);
      snapshotInfo.put(snapshotID.toString(), snapInfo);
      prevSnapshotID = snapshotID;
      snapshotInfoList.add(snapInfo);
    }
    chainManager = new SnapshotChainManager(omMetadataManager);
    assertFalse(chainManager.isSnapshotChainCorrupted());
    List<UUID> reverseChain = Lists.newArrayList(chainManager.iterator(true));
    Collections.reverse(reverseChain);
    List<UUID> forwardChain = Lists.newArrayList(chainManager.iterator(false));
    List<UUID> expectedChain = snapshotInfoList.stream().map(SnapshotInfo::getSnapshotId).collect(Collectors.toList());
    assertEquals(expectedChain, reverseChain);
    assertEquals(expectedChain, forwardChain);
    assertEquals(forwardChain, reverseChain);

  }

  private static Stream<? extends Arguments> invalidSnapshotChain() {
    List<UUID> nodes = IntStream.range(0, 5)
        .mapToObj(i -> UUID.randomUUID())
        .collect(Collectors.toList());
    return Stream.of(
        Arguments.of(nodes, Named.of("Disconnected Snapshot Chain",
            ImmutableMap.of(
                nodes.get(1), nodes.get(0),
                nodes.get(2), nodes.get(1),
                nodes.get(4), nodes.get(3)))),
        Arguments.of(nodes, Named.of("Complete Cyclic Snapshot Chain",
            ImmutableMap.of(
                nodes.get(0), nodes.get(4),
                nodes.get(1), nodes.get(0),
                nodes.get(2), nodes.get(1),
                nodes.get(3), nodes.get(2),
                nodes.get(4), nodes.get(3)))),
        Arguments.of(nodes, Named.of("Partial Cyclic Snapshot Chain",
            ImmutableMap.of(
                nodes.get(0), nodes.get(3),
                nodes.get(1), nodes.get(0),
                nodes.get(2), nodes.get(1),
                nodes.get(3), nodes.get(2),
                nodes.get(4), nodes.get(3)))),
        Arguments.of(nodes, Named.of("Diverged Snapshot Chain",
            ImmutableMap.of(nodes.get(1), nodes.get(0),
                nodes.get(2), nodes.get(1),
                nodes.get(3), nodes.get(2),
                nodes.get(4), nodes.get(2))))
    );
  }

  @ParameterizedTest
  @MethodSource("invalidSnapshotChain")
  public void testInvalidGlobalChainFromLoadFromTable(
      List<UUID> snapshotIDs, Map<UUID, UUID> snapshotChain) throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    for (UUID snapshotID : snapshotIDs) {
      snapshotInfo.put(snapshotID.toString(),
          createSnapshotInfo(snapshotID, snapshotChain.get(snapshotID),
              snapshotChain.get(snapshotID), System.currentTimeMillis()));
    }
    chainManager = new SnapshotChainManager(omMetadataManager);

    assertTrue(chainManager.isSnapshotChainCorrupted());

    SnapshotInfo snapInfo = createSnapshotInfo(UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        System.currentTimeMillis());
    IOException createException = assertThrows(IOException.class,
            () -> chainManager.addSnapshot(snapInfo));
    assertEquals("Snapshot chain is corrupted.", createException.getMessage());
    if (!snapshotIDs.isEmpty()) {
      IOException deleteException =
          assertThrows(IOException.class,
              () -> chainManager.deleteSnapshot(
                  snapshotInfo.get(snapshotIDs.get(0).toString())));
      assertEquals("Snapshot chain is corrupted.",
          deleteException.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("invalidSnapshotChain")
  public void testInvalidChainFromLoadFromTable(List<UUID> snapshotIDs,
      Map<UUID, UUID> snapshotChain) throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    UUID prevSnapshotId = null;
    for (UUID snapshotID : snapshotIDs) {
      snapshotInfo.put(snapshotID.toString(),
          createSnapshotInfo(snapshotID, snapshotChain.get(snapshotID),
              prevSnapshotId, System.currentTimeMillis()));
      prevSnapshotId = snapshotID;
    }
    chainManager = new SnapshotChainManager(omMetadataManager);
    assertTrue(chainManager.isSnapshotChainCorrupted());

    SnapshotInfo snapInfo = createSnapshotInfo(UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        System.currentTimeMillis());
    IOException createException = assertThrows(IOException.class,
            () -> chainManager.addSnapshot(snapInfo));
    assertEquals("Snapshot chain is corrupted.", createException.getMessage());
    if (!snapshotIDs.isEmpty()) {
      IOException deleteException =
          assertThrows(IOException.class,
              () -> chainManager.deleteSnapshot(
                  snapshotInfo.get(snapshotIDs.get(0).toString())));
      assertEquals("Snapshot chain is corrupted.",
          deleteException.getMessage());
    }
  }
}
