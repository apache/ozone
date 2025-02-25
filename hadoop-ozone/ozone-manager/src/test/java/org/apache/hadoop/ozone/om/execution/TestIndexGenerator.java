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

package org.apache.hadoop.ozone.om.execution;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.MANAGED_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests Index generator changes.
 */
public class TestIndexGenerator {
  @Test
  public void testIndexGenerator() throws Exception {
    OzoneManager om = mock(OzoneManager.class);
    OMMetadataManager metaManager = mock(OMMetadataManager.class);
    when(om.getMetadataManager()).thenReturn(metaManager);
    OMLayoutVersionManager verManager = mock(OMLayoutVersionManager.class);
    when(verManager.isAllowed(Mockito.eq(MANAGED_INDEX))).thenReturn(true);
    when(om.getVersionManager()).thenReturn(verManager);
    Table<String, TransactionInfo> txInfoTable = mock(Table.class);
    when(metaManager.getTransactionInfoTable()).thenReturn(txInfoTable);
    TransactionInfo txInfo = TransactionInfo.valueOf(-1, 100);
    when(txInfoTable.get(anyString())).thenReturn(txInfo);
    IndexGenerator indexGenerator = new IndexGenerator(om);
    assertEquals(indexGenerator.nextIndex(), 101);

    // save index and verify after change leader, for next index
    BatchOperation batchOpr = mock(BatchOperation.class);
    indexGenerator.saveIndex(batchOpr, 102);
    indexGenerator.onLeaderChange();
    assertEquals(indexGenerator.nextIndex(), 103);
  }

  @Test
  public void testUpgradeIndexGenerator() throws Exception {
    OzoneManager om = mock(OzoneManager.class);
    OMMetadataManager metaManager = mock(OMMetadataManager.class);
    when(om.getMetadataManager()).thenReturn(metaManager);
    OMLayoutVersionManager verManager = mock(OMLayoutVersionManager.class);
    when(om.getVersionManager()).thenReturn(verManager);
    when(verManager.needsFinalization()).thenReturn(true);
    Table<String, TransactionInfo> txInfoTable = mock(Table.class);
    when(metaManager.getTransactionInfoTable()).thenReturn(txInfoTable);
    DBStore dbstore = mock(DBStore.class);
    when(metaManager.getStore()).thenReturn(dbstore);
    BatchOperation batchOpr = mock(BatchOperation.class);
    when(dbstore.initBatchOperation()).thenReturn(batchOpr);
    TransactionInfo txInfo = TransactionInfo.valueOf(-1, 110);
    when(txInfoTable.get(anyString())).thenReturn(null).thenReturn(txInfo);
    when(txInfoTable.getSkipCache(anyString())).thenReturn(txInfo);
    IndexGenerator indexGenerator = new IndexGenerator(om);
    assertEquals(indexGenerator.nextIndex(), -1);

    indexGenerator.saveIndex(batchOpr, 114);
    indexGenerator.onLeaderChange();
    assertEquals(indexGenerator.nextIndex(), -1);
    
    // check ExecutionContext behavior
    ExecutionContext executionContext = ExecutionContext.of(indexGenerator.nextIndex(), txInfo.getTermIndex());
    assertEquals(executionContext.getIndex(), txInfo.getTermIndex().getIndex());
    

    // save index and verify after change leader, for next index
    indexGenerator.finalizeIndexGeneratorFeature();
    assertEquals(indexGenerator.nextIndex(), 111);

    // save index and verify after change leader, for next index
    indexGenerator.saveIndex(batchOpr, 114);
    indexGenerator.onLeaderChange();
    assertEquals(indexGenerator.nextIndex(), 115);

    executionContext = ExecutionContext.of(indexGenerator.nextIndex(), txInfo.getTermIndex());
    assertEquals(executionContext.getIndex(), 116);
  }
}
