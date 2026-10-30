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

package org.apache.hadoop.hdds.scm.server.upgrade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClearFinalizingStateScmUpgradeAction}.
 */
class TestClearFinalizingStateScmUpgradeAction {

  private static final String LEGACY_FINALIZING_KEY = "#FINALIZING";

  @Test
  void testRemovesFinalizingKey() throws Exception {
    @SuppressWarnings("unchecked")
    Table<String, String> metaTable = mock(Table.class);
    DBTransactionBuffer buffer = mock(DBTransactionBuffer.class);

    SCMMetadataStore metadataStore = mock(SCMMetadataStore.class);
    when(metadataStore.getMetaTable()).thenReturn(metaTable);

    SCMHAManager haManager = mock(SCMHAManager.class);
    when(haManager.getDBTransactionBuffer()).thenReturn(buffer);

    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getScmHAManager()).thenReturn(haManager);

    new ClearFinalizingStateScmUpgradeAction().execute(scm);

    verify(buffer).removeFromBuffer(metaTable, LEGACY_FINALIZING_KEY);
  }

  @Test
  void testIdempotent() throws Exception {
    // removeFromBuffer on an absent key is a no-op; call execute twice and expect no exception.
    @SuppressWarnings("unchecked")
    Table<String, String> metaTable = mock(Table.class);
    DBTransactionBuffer buffer = mock(DBTransactionBuffer.class);

    SCMMetadataStore metadataStore = mock(SCMMetadataStore.class);
    when(metadataStore.getMetaTable()).thenReturn(metaTable);

    SCMHAManager haManager = mock(SCMHAManager.class);
    when(haManager.getDBTransactionBuffer()).thenReturn(buffer);

    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getScmHAManager()).thenReturn(haManager);

    ClearFinalizingStateScmUpgradeAction action = new ClearFinalizingStateScmUpgradeAction();
    action.execute(scm);
    action.execute(scm);

    // Called twice, once per execution.
    verify(buffer, org.mockito.Mockito.times(2)).removeFromBuffer(metaTable, LEGACY_FINALIZING_KEY);
  }
}
