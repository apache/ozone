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

package org.apache.hadoop.hdds.scm.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.FlushedTransactionInfo;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests SCM MetadataManager.
 */
public class TestSCMMetadataStore {
  private SCMMetadataStore scmMetadataStore;
  private OzoneConfiguration ozoneConfiguration;
  @TempDir
  private File folder;

  @BeforeEach
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.getPath());
    scmMetadataStore = new SCMMetadataStoreImpl(ozoneConfiguration);
  }

  @Test
  public void testFlushedTransactionTable() throws RocksDatabaseException, CodecException {
    List<FlushedTransactionInfo> flushedTransactionInfos = new ArrayList<>();
    Assertions.assertThrows(AssertionError.class, () -> {
      scmMetadataStore.getFlushedTransactionsTable().put(-100L, FlushedTransactionInfo.valueOf(-100L, -100L));
    });
    for (long i = 10; i >= 0; i--) {
      FlushedTransactionInfo flushedTransactionInfo = FlushedTransactionInfo.valueOf(i, i);
      scmMetadataStore.getFlushedTransactionsTable().put(i, flushedTransactionInfo);
      flushedTransactionInfos.add(flushedTransactionInfo);
    }
    flushedTransactionInfos.sort(Comparator.comparingLong(FlushedTransactionInfo::getTransactionIndex));
    List<FlushedTransactionInfo> dbList = new ArrayList<>();
    try (Table.KeyValueIterator<Long, FlushedTransactionInfo> itr =
             scmMetadataStore.getFlushedTransactionsTable().iterator()) {
      while (itr.hasNext()) {
        dbList.add(itr.next().getValue());
      }
    }
    assertEquals(flushedTransactionInfos, dbList);
  }

}
