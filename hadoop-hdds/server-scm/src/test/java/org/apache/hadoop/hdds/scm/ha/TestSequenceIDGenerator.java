/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMDBTransactionBufferImpl;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SEQUENCE_ID_BATCH_SIZE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SequenceIdGenerator}.
 */
public class TestSequenceIDGenerator {
  @Test
  public void testSequenceIDGenUponNonRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SCMHAManager scmHAManager = SCMHAManagerStub
        .getInstance(true, new SCMDBTransactionBufferImpl());

    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 1000]
    Assertions.assertEquals(1L, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(2L, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(3L, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(1L, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(2L, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(3L, sequenceIdGen.getNextId("otherKey"));

    // default batchSize is 1000, the next batch is [1001, 2000]
    sequenceIdGen.invalidateBatch();
    Assertions.assertEquals(1001, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(1002, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(1003, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(1001, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(1002, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(1003, sequenceIdGen.getNextId("otherKey"));

    // default batchSize is 1000, the next batch is [2001, 3000]
    sequenceIdGen.invalidateBatch();
    Assertions.assertEquals(2001, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(2002, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(2003, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(2001, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(2002, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(2003, sequenceIdGen.getNextId("otherKey"));
  }

  @Test
  public void testSequenceIDGenUponRatis() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    
    // change batchSize to 100
    conf.setInt(OZONE_SCM_SEQUENCE_ID_BATCH_SIZE, 100);

    SCMMetadataStore scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    // the first batch is [1, 100]
    Assertions.assertEquals(1L, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(2L, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(3L, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(1L, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(2L, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(3L, sequenceIdGen.getNextId("otherKey"));

    // the next batch is [101, 200]
    sequenceIdGen.invalidateBatch();
    Assertions.assertEquals(101, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(102, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(103, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(101, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(102, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(103, sequenceIdGen.getNextId("otherKey"));

    // the next batch is [201, 300]
    sequenceIdGen.invalidateBatch();
    Assertions.assertEquals(201, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(202, sequenceIdGen.getNextId("someKey"));
    Assertions.assertEquals(203, sequenceIdGen.getNextId("someKey"));

    Assertions.assertEquals(201, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(202, sequenceIdGen.getNextId("otherKey"));
    Assertions.assertEquals(203, sequenceIdGen.getNextId("otherKey"));
  }

  @Test
  public void testSequenceIDGenUponRatisWhenCurrentScmIsNotALeader()
      throws Exception {
    int batchSize = 100;
    OzoneConfiguration conf = SCMTestUtils.getConf();
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
          SCMHAManager scmhaManager, Table<String, Long> sequenceIdTable) {
        Preconditions.checkNotNull(scmhaManager);
        return stateManager;
      }
    };

    Assertions.assertEquals(1L, sequenceIdGen.getNextId("someKey"));

    // Simulation currently this SCM is not a leader node,
    // So this SCM can only allocate IDs within the current batch
    // ([1, batchSize]), does not allow the allocation of IDs for the next batch
    // ([batchSize + 1, batchSize * 2])
    when(stateManager.allocateBatch(anyString(), anyLong(), anyLong()))
        .thenThrow(new SCMException(SCMException.ResultCodes.SCM_NOT_LEADER));

    for (int i = 0; i < batchSize * 3; i++) {
      try {
        long nextID = sequenceIdGen.getNextId("someKey");
        if (nextID > batchSize) {
          Assertions.fail("Should not allocate a blockID: " + nextID +
              " that exceeds the current Batch: " + batchSize);
        }
      } catch (Exception e) {
        // ignore
      }
    }
  }
}
