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
package org.apache.hadoop.ozone.om.execution;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests Index generator changes.
 */
public class TestIndexGenerator {
  @Test
  public void testIndexGenerator() throws Exception {
    OzoneManager om = Mockito.mock(OzoneManager.class);
    OMMetadataManager metaManager = Mockito.mock(OMMetadataManager.class);
    Mockito.when(om.getMetadataManager()).thenReturn(metaManager);
    OMLayoutVersionManager verManager = Mockito.mock(OMLayoutVersionManager.class);
    Mockito.when(om.getVersionManager()).thenReturn(verManager);
    Table<String, TransactionInfo> txInfoTable = Mockito.mock(Table.class);
    Mockito.when(metaManager.getTransactionInfoTable()).thenReturn(txInfoTable);
    TransactionInfo txInfo = TransactionInfo.valueOf(-1, 100);
    Mockito.when(txInfoTable.get(Mockito.anyString())).thenReturn(txInfo);
    IndexGenerator indexGenerator = new IndexGenerator(om);
    Assertions.assertEquals(indexGenerator.nextIndex(), 101);

    // save index and verify after change leader, for next index
    BatchOperation batchOpr = Mockito.mock(BatchOperation.class);
    indexGenerator.saveIndex(batchOpr, 102);
    indexGenerator.changeLeader();
    Assertions.assertEquals(indexGenerator.nextIndex(), 103);
  }

  @Test
  public void testUpgradeIndexGenerator() throws Exception {
    OzoneManager om = Mockito.mock(OzoneManager.class);
    OMMetadataManager metaManager = Mockito.mock(OMMetadataManager.class);
    Mockito.when(om.getMetadataManager()).thenReturn(metaManager);
    OMLayoutVersionManager verManager = Mockito.mock(OMLayoutVersionManager.class);
    Mockito.when(om.getVersionManager()).thenReturn(verManager);
    Mockito.when(verManager.needsFinalization()).thenReturn(true);
    Table<String, TransactionInfo> txInfoTable = Mockito.mock(Table.class);
    Mockito.when(metaManager.getTransactionInfoTable()).thenReturn(txInfoTable);
    TransactionInfo txInfo = TransactionInfo.valueOf(-1, 110);
    Mockito.when(txInfoTable.get(Mockito.anyString())).thenReturn(null).thenReturn(txInfo);
    Mockito.when(txInfoTable.getSkipCache(Mockito.anyString())).thenReturn(txInfo);
    IndexGenerator indexGenerator = new IndexGenerator(om);
    Assertions.assertEquals(indexGenerator.nextIndex(), -1);

    BatchOperation batchOpr = Mockito.mock(BatchOperation.class);
    indexGenerator.saveIndex(batchOpr, 114);
    indexGenerator.changeLeader();
    Assertions.assertEquals(indexGenerator.nextIndex(), -1);
    
    // check ExecutionContext behavior
    ExecutionContext executionContext = ExecutionContext.of(indexGenerator.nextIndex(), txInfo.getTermIndex());
    Assertions.assertEquals(executionContext.getIndex(), txInfo.getTermIndex().getIndex());
    

    // save index and verify after change leader, for next index
    indexGenerator.finalizeIndexGeneratorFeature();
    Assertions.assertEquals(indexGenerator.nextIndex(), 111);

    // save index and verify after change leader, for next index
    indexGenerator.saveIndex(batchOpr, 114);
    indexGenerator.changeLeader();
    Assertions.assertEquals(indexGenerator.nextIndex(), 115);

    executionContext = ExecutionContext.of(indexGenerator.nextIndex(), txInfo.getTermIndex());
    Assertions.assertEquals(executionContext.getIndex(), 116);
  }
}
