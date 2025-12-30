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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        .build();
  }

  @Test
  public void testTableExists() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    assertTrue(snapshotInfo.isEmpty());
  }

  @Test
  public void testAddNewSnapshot() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    assertEquals(EXPECTED_SNAPSHOT_ID,
        snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getSnapshotId());
  }

  @Test
  public void testDeleteSnapshotInfo() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();

    assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, createSnapshotInfo());
    assertTrue(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.delete(EXPECTED_SNAPSHOT_KEY);
    assertFalse(snapshotInfo.isExist(EXPECTED_SNAPSHOT_KEY));
  }

  @Test
  public void testSnapshotSSTFilteredFlag() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
            omMetadataManager.getSnapshotInfoTable();
    SnapshotInfo info = createSnapshotInfo();
    info.setSstFiltered(false);
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    assertFalse(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).isSstFiltered());
    info.setSstFiltered(true);
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    assertTrue(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).isSstFiltered());
  }

  @Test
  public void testLastTransactionInfo() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    SnapshotInfo info = createSnapshotInfo();
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    assertNull(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getLastTransactionInfo());
    // checking if true value is returned when snapshot is null.
    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, (SnapshotInfo)null));
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(0, 0));
    // Checking if changes have been flushed when lastTransactionInfo is null
    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, info));
    TermIndex termIndex = TermIndex.valueOf(1, 1);
    info.setLastTransactionInfo(TransactionInfo.valueOf(termIndex).toByteString());
    // Checking if changes to snapshot object has been updated but not updated on cache or disk.
    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));
    snapshotInfo.addCacheEntry(new CacheKey<>(EXPECTED_SNAPSHOT_KEY), CacheValue.get(termIndex.getIndex(), info));

    assertEquals(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getLastTransactionInfo(), info.getLastTransactionInfo());

    // Checking if changes have not been flushed when snapshot last transaction info is behind OmTransactionTable value.
    assertFalse(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));
    omMetadataManager.getTransactionInfoTable().addCacheEntry(new CacheKey<>(OzoneConsts.TRANSACTION_INFO_KEY),
        CacheValue.get(termIndex.getIndex(), TransactionInfo.valueOf(1, 1)));
    assertFalse(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));

    // Checking changes are flushed when transaction is equal.
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(1, 1));


    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));
    // Checking changes are flushed when transactionIndex is greater .
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(1, 2));
    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));
    // Checking changes are flushed when both term & transactionIndex is greater.
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(2, 2));
    assertTrue(OmSnapshotManager.areSnapshotChangesFlushedToDB(omMetadataManager, EXPECTED_SNAPSHOT_KEY));
  }

  @Test
  public void testCreateTransactionInfo() throws Exception {
    Table<String, SnapshotInfo> snapshotInfo =
        omMetadataManager.getSnapshotInfoTable();
    SnapshotInfo info = createSnapshotInfo();
    snapshotInfo.put(EXPECTED_SNAPSHOT_KEY, info);
    assertNull(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getCreateTransactionInfo());
    // checking if true value is returned when snapshot is null.
    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, null));
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(0, 0));
    // Checking if changes have been flushed when createTransactionInfo is null
    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, info));
    TermIndex termIndex = TermIndex.valueOf(1, 1);
    info.setCreateTransactionInfo(TransactionInfo.valueOf(termIndex).toByteString());
    // Checking if changes to snapshot object has been updated but not updated on cache or disk.
    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, snapshotInfo.get(EXPECTED_SNAPSHOT_KEY)));
    snapshotInfo.addCacheEntry(new CacheKey<>(EXPECTED_SNAPSHOT_KEY), CacheValue.get(termIndex.getIndex(), info));

    assertEquals(snapshotInfo.get(EXPECTED_SNAPSHOT_KEY).getCreateTransactionInfo(), info.getCreateTransactionInfo());
    SnapshotInfo tableSnapshotInfo = snapshotInfo.get(EXPECTED_SNAPSHOT_KEY);
    // Checking if changes have not been flushed when snapshot last transaction info is behind OmTransactionTable value.
    assertFalse(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, tableSnapshotInfo));
    omMetadataManager.getTransactionInfoTable().addCacheEntry(new CacheKey<>(OzoneConsts.TRANSACTION_INFO_KEY),
        CacheValue.get(termIndex.getIndex(), TransactionInfo.valueOf(1, 1)));
    assertFalse(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, tableSnapshotInfo));

    // Checking changes are flushed when transaction is equal.
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(1, 1));


    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, tableSnapshotInfo));
    // Checking changes are flushed when transactionIndex is greater .
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(1, 2));
    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, tableSnapshotInfo));
    // Checking changes are flushed when both term & transactionIndex is greater.
    omMetadataManager.getTransactionInfoTable().put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(2, 2));
    assertTrue(OmSnapshotManager.isSnapshotFlushedToDB(omMetadataManager, tableSnapshotInfo));
  }
}
