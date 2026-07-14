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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link SequenceIdGenerator}.
 */
public class TestSequenceIDGenerator {

  @TempDir
  private File testDir;

  @Test
  public void testSequenceIDGenUponNonRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SCMHAManager scmHAManager = SCMHAManagerStub
        .getInstance(true, new SCMDBTransactionBufferImpl());

    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 1000]
    assertEquals(1L, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(2L, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(3L, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(1L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(2L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(3L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));

    // default batchSize is 1000, the next batch is [1001, 2000]
    sequenceIdGen.invalidateBatch();
    assertEquals(1001, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(1002, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(1003, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(1001, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(1002, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(1003, sequenceIdGen.getNextId(SequenceIdType.delTxnId));

    // default batchSize is 1000, the next batch is [2001, 3000]
    sequenceIdGen.invalidateBatch();
    assertEquals(2001, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(2002, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(2003, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(2001, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(2002, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(2003, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
  }

  @Test
  public void testSequenceIDGenUponRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    
    // change batchSize to 100
    conf.setInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE, 100);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 100]
    assertEquals(1L, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(2L, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(3L, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(1L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(2L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(3L, sequenceIdGen.getNextId(SequenceIdType.delTxnId));

    // the next batch is [101, 200]
    sequenceIdGen.invalidateBatch();
    assertEquals(101, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(102, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(103, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(101, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(102, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(103, sequenceIdGen.getNextId(SequenceIdType.delTxnId));

    // the next batch is [201, 300]
    sequenceIdGen.invalidateBatch();
    assertEquals(201, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(202, sequenceIdGen.getNextId(SequenceIdType.localId));
    assertEquals(203, sequenceIdGen.getNextId(SequenceIdType.localId));

    assertEquals(201, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(202, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
    assertEquals(203, sequenceIdGen.getNextId(SequenceIdType.delTxnId));
  }

  @Test
  public void testSequenceIDGenUponRatisWhenCurrentScmIsNotALeader()
      throws Exception {
    int batchSize = 100;
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    conf.setInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE, batchSize);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);
    SCMHAManager scmHAManager = SCMHAManagerStub
        .getInstance(true, new SCMDBTransactionBufferImpl());

    SequenceIdGenerator.StateManager stateManager =
        spy(new SequenceIdGenerator.StateManagerImpl.Builder()
            .setRatisServer(scmHAManager.getRatisServer())
            .setDBTransactionBuffer(scmHAManager.getDBTransactionBuffer())
            .setSequenceIdTable(scmMetadataStore.getSequenceIdTable())
            .build());
    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable()) {
      @Override
      public StateManager createStateManager(
          SCMHAManager scmhaManager, Table<SequenceIdType, Long> sequenceIdTable) {
        Objects.requireNonNull(scmhaManager, "scmhaManager == null");
        return stateManager;
      }
    };

    assertEquals(1L, sequenceIdGen.getNextId(SequenceIdType.localId));

    // Simulation currently this SCM is not a leader node,
    // So this SCM can only allocate IDs within the current batch
    // ([1, batchSize]), does not allow the allocation of IDs for the next batch
    // ([batchSize + 1, batchSize * 2])
    doThrow(new SCMException(SCMException.ResultCodes.SCM_NOT_LEADER))
        .when(stateManager).allocateBatch(anyString(), anyLong(), anyLong());

    for (int i = 0; i < batchSize * 3; i++) {
      try {
        long nextID = sequenceIdGen.getNextId(SequenceIdType.localId);
        if (nextID > batchSize) {
          fail("Should not allocate a blockID: " + nextID +
              " that exceeds the current Batch: " + batchSize);
        }
      } catch (Exception e) {
        // ignore
      }
    }
  }

  @Test
  public void testAllocateBatchFromDBWhenMissingInMap() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);
    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    // Create the StateManager directly using its Builder
    SequenceIdGenerator.StateManager stateManager =
        new SequenceIdGenerator.StateManagerImpl.Builder()
            .setRatisServer(scmHAManager.getRatisServer())
            .setDBTransactionBuffer(scmHAManager.getDBTransactionBuffer())
            .setSequenceIdTable(scmMetadataStore.getSequenceIdTable())
            .build();

    SequenceIdType idType = SequenceIdType.localId;
    // Verify initial state from empty DB
    Assertions.assertNull(stateManager.getLastId(idType));

    // Allocate a new batch, which puts 100L into the sequenceIdToLastIdMap map
    assertTrue(stateManager.allocateBatch(idType.name(), 0L, 100L));
    // Verify the map was updated
    assertEquals(100L, stateManager.getLastId(idType));

    // Allocate a new batch, which puts 100L into the sequenceIdToLastIdMap map
    assertTrue(stateManager.allocateBatch(idType.name(), 100L, 200L));
    // Verify the map was updated
    assertEquals(200L, stateManager.getLastId(idType));

    // This call should fail because expectedLastId in db should be (200L)
    // But we are passing 0L
    assertFalse(stateManager.allocateBatch(idType.name(), 0L, 100L));
  }

  @Test
  public void testReinitializePopulatesSequenceIdMapFromDB() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);
    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    SequenceIdType idType = SequenceIdType.containerId;
    // Simulate an SCM restart by writing a raw String directly to the database.
    scmMetadataStore.getSequenceIdTable().put(idType, 100L);

    // Create the StateManager directly using its Builder
    SequenceIdGenerator.StateManager stateManager =
        new SequenceIdGenerator.StateManagerImpl.Builder()
            .setRatisServer(scmHAManager.getRatisServer())
            .setDBTransactionBuffer(scmHAManager.getDBTransactionBuffer())
            .setSequenceIdTable(scmMetadataStore.getSequenceIdTable())
            .build();

    // Check if reinitialize() correctly converts DB key into SequenceIdType Enums
    // for the sequenceIdToLastIdMap used.
    stateManager.reinitialize(scmMetadataStore.getSequenceIdTable());

    assertEquals(100L, stateManager.getLastId(idType));
    assertTrue(stateManager.allocateBatch(idType.name(), 100L, 1100L));
    assertEquals(1100L, stateManager.getLastId(idType));
  }

  @Test
  public void testAllocateBatchFailsOnUnknownSequenceId() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);
    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    // Create the StateManager directly using its Builder
    SequenceIdGenerator.StateManager stateManager =
        new SequenceIdGenerator.StateManagerImpl.Builder()
            .setRatisServer(scmHAManager.getRatisServer())
            .setDBTransactionBuffer(scmHAManager.getDBTransactionBuffer())
            .setSequenceIdTable(scmMetadataStore.getSequenceIdTable())
            .build();

    try {
      // sequenceIdName string must match one of the predefined Enums.
      // Passing an invalid string should immediately throw an exception before hitting the db.
      stateManager.allocateBatch("unknownSequenceId", 0L, 1L);
      fail("Expected allocateBatch to reject an unknown sequence id");
    } catch (Exception e) {
      // ignore
    }
  }
}
