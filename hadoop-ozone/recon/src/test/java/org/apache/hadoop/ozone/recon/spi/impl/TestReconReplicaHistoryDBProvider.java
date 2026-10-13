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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.REPLICA_HISTORY_V2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerChecksums;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistory;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.jooq.impl.DefaultConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests startup migration and the independent lifetime of container replica history.
 */
class TestReconReplicaHistoryDBProvider {
  @TempDir
  private Path temporaryFolder;

  private OzoneConfiguration configuration;
  private ReconDBProvider reconDBProvider;
  private ReconReplicaHistoryDBProvider historyDBProvider;

  @BeforeEach
  void setUp() {
    configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_DB_DIR, temporaryFolder.toString());
    reconDBProvider = new ReconDBProvider(configuration, new ReconUtils());
  }

  @AfterEach
  void tearDown() throws Exception {
    if (historyDBProvider != null) {
      historyDBProvider.close();
    }
    reconDBProvider.close();
  }

  @Test
  void testMigratesLegacyHistoryWithoutChangingSource() throws Exception {
    Table<Long, ContainerReplicaHistoryList> legacyTable = REPLICA_HISTORY_V2.getTable(reconDBProvider.getDbStore());
    ContainerReplicaHistoryList history = new ContainerReplicaHistoryList(Arrays.asList(newHistory(), newHistory()));
    for (long id = 1; id <= 2000; id++) {
      legacyTable.put(id, history);
    }

    openHistoryDB();
    for (long id = 1; id <= 2000; id++) {
      assertEquals(history.toProto(), historyDBProvider.getReplicaHistoryTable().get(id).toProto());
      assertEquals(history.toProto(), legacyTable.get(id).toProto());
    }
    assertFalse(historyDBProvider.getDbStore().getTableNames()
        .containsValue(ReconDBDefinition.CONTAINER_KEY.getName()));
  }

  @Test
  void testFreshDatabaseAndRestartSkipMigration() throws Exception {
    openHistoryDB();
    assertTrue(historyDBProvider.getReplicaHistoryTable().isEmpty());
    ContainerReplicaHistoryList history = new ContainerReplicaHistoryList(Collections.singletonList(newHistory()));
    historyDBProvider.getReplicaHistoryTable().put(1L, history);
    historyDBProvider.close();

    DBStore sourceStore = mock(DBStore.class);
    historyDBProvider = new ReconReplicaHistoryDBProvider(configuration);
    historyDBProvider.migrateReplicaHistory(sourceStore);
    verifyNoInteractions(sourceStore);
    assertEquals(history.toProto(), historyDBProvider.getReplicaHistoryTable().get(1L).toProto());
  }

  @Test
  void testRetriesPartiallyMigratedDatabase() throws Exception {
    ContainerReplicaHistoryList history = new ContainerReplicaHistoryList(Collections.singletonList(newHistory()));
    Table<Long, ContainerReplicaHistoryList> source = REPLICA_HISTORY_V2.getTable(reconDBProvider.getDbStore());
    source.put(1L, history);
    source.put(2L, history);
    try (DBStore partialStore = DBStoreBuilder.createDBStore(configuration, new ReconReplicaHistoryDBDefinition())) {
      REPLICA_HISTORY_V2.getTable(partialStore).put(1L,
          new ContainerReplicaHistoryList(Collections.singletonList(newHistory())));
    }

    openHistoryDB();
    assertEquals(history.toProto(), historyDBProvider.getReplicaHistoryTable().get(1L).toProto());
    assertEquals(history.toProto(), historyDBProvider.getReplicaHistoryTable().get(2L).toProto());
  }

  @Test
  void testFailedMigrationCanBeRetriedAfterRestart() throws Exception {
    ContainerReplicaHistoryList history = new ContainerReplicaHistoryList(Collections.singletonList(newHistory()));
    REPLICA_HISTORY_V2.getTable(reconDBProvider.getDbStore()).put(1L, history);
    Table<byte[], byte[]> sourceTable = reconDBProvider.getDbStore().getTable(REPLICA_HISTORY_V2.getName());
    Table<byte[], byte[]> failingTable = mock(Table.class);
    doAnswer(invocation -> {
      sourceTable.dumpToFileWithPrefix(invocation.getArgument(0), null);
      throw new IOException("Interrupted migration");
    }).when(failingTable).dumpToFileWithPrefix(any(), isNull());
    DBStore failingSource = mock(DBStore.class);
    when(failingSource.getTable(anyString())).thenReturn(failingTable);
    historyDBProvider = new ReconReplicaHistoryDBProvider(configuration);
    assertThrows(IOException.class, () -> historyDBProvider.migrateReplicaHistory(failingSource));
    historyDBProvider.close();
    try (DBStore destination = DBStoreBuilder.createDBStore(configuration, new ReconReplicaHistoryDBDefinition())) {
      assertTrue(ReconReplicaHistoryDBDefinition.METADATA.getTable(destination).isEmpty());
      assertTrue(REPLICA_HISTORY_V2.getTable(destination).isEmpty());
    }
    assertEquals(history.toProto(), REPLICA_HISTORY_V2.getTable(reconDBProvider.getDbStore()).get(1L).toProto());

    openHistoryDB();
    assertEquals(history.toProto(), historyDBProvider.getReplicaHistoryTable().get(1L).toProto());
  }

  @Test
  void testHistorySurvivesRepeatedStagedDbReplacement() throws Exception {
    openHistoryDB();
    ReconContainerMetadataManager manager = newManager();
    ContainerReplicaHistory history = newHistory();
    manager.storeContainerReplicaHistory(1L, Collections.singletonMap(history.getId(), history));
    DBStore historyStore = historyDBProvider.getDbStore();
    Table<Long, ContainerReplicaHistoryList> historyTable = historyDBProvider.getReplicaHistoryTable();

    for (int i = 0; i < 3; i++) {
      ReconDBProvider stagedProvider = reconDBProvider.getStagedReconDBProvider();
      ReconContainerMetadataManager stagedManager = manager.getStagedReconContainerMetadataManager(
          stagedProvider.getDbStore());
      try (RDBBatchOperation batch = RDBBatchOperation.newAtomicOperation()) {
        stagedManager.batchStoreContainerKeyCounts(batch, 1L, (long) i + 1);
        stagedManager.commitBatchOperation(batch);
      }
      // History writes continue while OM-derived data is being rebuilt in staging.
      ContainerReplicaHistory anotherHistory = newHistory();
      Map<DatanodeID, ContainerReplicaHistory> histories = manager.getContainerReplicaHistory(1L);
      histories.put(anotherHistory.getId(), anotherHistory);
      manager.storeContainerReplicaHistory(1L, histories);
      assertTrue(REPLICA_HISTORY_V2.getTable(stagedProvider.getDbStore()).isEmpty());
      reconDBProvider.replaceStagedDb(stagedProvider);
      manager.reinitialize(reconDBProvider);

      assertSame(historyStore, historyDBProvider.getDbStore());
      assertSame(historyTable, historyDBProvider.getReplicaHistoryTable());
      assertFalse(historyStore.isClosed());
      assertEquals(i + 1, manager.getKeyCountForContainer(1L));
      assertEquals(i + 2, manager.getContainerReplicaHistory(1L).size());
      assertEquals(history.toProto(), manager.getContainerReplicaHistory(1L).get(history.getId()).toProto());
    }
    historyDBProvider.close();
    openHistoryDB();
    assertEquals(4, newManager().getContainerReplicaHistory(1L).size());
  }

  @Test
  void testShutdownBatchPreservesDepartedReplicas() throws Exception {
    openHistoryDB();
    ReconContainerMetadataManager manager = newManager();
    ContainerReplicaHistory departed = newHistory();
    ContainerReplicaHistory current = newHistory();
    manager.storeContainerReplicaHistory(1L, Collections.singletonMap(departed.getId(), departed));
    manager.batchStoreContainerReplicaHistory(Collections.singletonMap(
        1L, Collections.singletonMap(current.getId(), current)));
    historyDBProvider.close();
    openHistoryDB();

    Map<DatanodeID, ContainerReplicaHistory> history = newManager().getContainerReplicaHistory(1L);
    assertEquals(2, history.size());
    assertEquals(departed.toProto(), history.get(departed.getId()).toProto());
    assertEquals(current.toProto(), history.get(current.getId()).toProto());
    assertNull(historyDBProvider.getReplicaHistoryTable().get(2L));
  }

  @ParameterizedTest
  @CsvSource({
      "10, 20, 30, 40, 10, 40, true",
      "30, 40, 10, 20, 10, 40, false",
      "10, 40, 30, 40, 10, 40, true"
  })
  void testShutdownBatchPreservesFirstSeenAndLatestReplicaInfo(long storedFirstSeen, long storedLastSeen,
      long currentFirstSeen, long currentLastSeen, long expectedFirstSeen, long expectedLastSeen,
      boolean currentFieldsExpected) throws Exception {
    openHistoryDB();
    ReconContainerMetadataManager manager = newManager();
    DatanodeID id = DatanodeID.randomID();
    ContainerReplicaHistory stored = new ContainerReplicaHistory(id, storedFirstSeen, storedLastSeen, 100L,
        "OPEN", ContainerChecksums.of(1111L, 0L));
    ContainerReplicaHistory current = new ContainerReplicaHistory(id, currentFirstSeen, currentLastSeen, 200L,
        "CLOSED", ContainerChecksums.of(2222L, 0L));
    manager.storeContainerReplicaHistory(1L, Collections.singletonMap(id, stored));
    manager.batchStoreContainerReplicaHistory(Collections.singletonMap(1L, Collections.singletonMap(id, current)));
    assertEquals(currentFirstSeen, current.getFirstSeenTime());
    assertEquals(currentLastSeen, current.getLastSeenTime());
    historyDBProvider.close();
    openHistoryDB();

    ContainerReplicaHistory expected = new ContainerReplicaHistory(id, expectedFirstSeen, expectedLastSeen,
        currentFieldsExpected ? 200L : 100L, currentFieldsExpected ? "CLOSED" : "OPEN",
        ContainerChecksums.of(currentFieldsExpected ? 2222L : 1111L, 0L));
    Map<DatanodeID, ContainerReplicaHistory> history = newManager().getContainerReplicaHistory(1L);
    assertEquals(1, history.size());
    assertEquals(expected.toProto(), history.get(id).toProto());
  }

  private void openHistoryDB() throws IOException {
    historyDBProvider = new ReconReplicaHistoryDBProvider(configuration);
    historyDBProvider.migrateReplicaHistory(reconDBProvider.getDbStore());
  }

  private ReconContainerMetadataManager newManager() {
    return new ReconContainerMetadataManagerImpl(reconDBProvider, historyDBProvider,
        new DefaultConfiguration(), mock(ReconOMMetadataManager.class));
  }

  private static ContainerReplicaHistory newHistory() {
    return new ContainerReplicaHistory(DatanodeID.randomID(), 10L, 20L, 30L,
        "CLOSED", ContainerChecksums.of(1234L, 0L));
  }
}
