/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * Class to test OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private OzoneManagerStateMachine ozoneManagerStateMachine;
  @Before
  public void setup() throws Exception {
    OzoneManagerRatisServer ozoneManagerRatisServer =
        Mockito.mock(OzoneManagerRatisServer.class);
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempDir.newFolder().getAbsolutePath().toString());

    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    when(ozoneManagerRatisServer.getOzoneManager()).thenReturn(ozoneManager);
    when(ozoneManager.getSnapshotInfo()).thenReturn(
        Mockito.mock(OMRatisSnapshotInfo.class));
    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(ozoneManagerRatisServer, false);
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 0);
  }

  @Test
  public void testLastAppliedIndex() {

    // Happy scenario.

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    Assert.assertEquals(0,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(1,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    List<Long> flushedEpochs = new ArrayList<>();

    // Add some apply transactions.
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0, 2);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0, 3);

    flushedEpochs.add(2L);
    flushedEpochs.add(3L);

    // call update last applied index
    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(3,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0L, 4L);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(4L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // Add some apply transactions.
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 5L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 6L);

    flushedEpochs.clear();
    flushedEpochs.add(5L);
    flushedEpochs.add(6L);
    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(6L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());


  }


  @Test
  public void testApplyTransactionsUpdateLastAppliedIndexCalledLate() {
    // Now try a scenario where 1,2,3 transactions are in applyTransactionMap
    // and updateLastAppliedIndex is not called for them, and before that
    // notifyIndexUpdate is called with transaction 4. And see now at the end
    // when updateLastAppliedIndex is called with epochs we have
    // lastAppliedIndex as 4 or not.

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0, 1);
    Assert.assertEquals(0,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(1,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());



    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 2L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 3L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 4L);



    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyTermIndexUpdated(0L, 5L);

  // Still it should be zero, as for 2,3,4 updateLastAppliedIndex is not yet
    // called so the lastAppliedIndex will be at older value.
    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(1L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    List<Long> flushedEpochs = new ArrayList<>();


    flushedEpochs.add(2L);
    flushedEpochs.add(3L);
    flushedEpochs.add(4L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(5L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

  }


  @Test
  public void testLastAppliedIndexWithMultipleExecutors() {

    // first flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 1L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 2L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 4L);

    List<Long> flushedEpochs = new ArrayList<>();


    flushedEpochs.add(1L);
    flushedEpochs.add(2L);
    flushedEpochs.add(4L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(2L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());




    // 2nd flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 3L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 5L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 6L);

    flushedEpochs.clear();
    flushedEpochs.add(3L);
    flushedEpochs.add(5L);
    flushedEpochs.add(6L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(6L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

    // 3rd flush batch
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 7L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 8L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 9L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 10L);

    flushedEpochs.clear();
    flushedEpochs.add(7L);
    flushedEpochs.add(8L);
    flushedEpochs.add(9L);
    flushedEpochs.add(10L);

    ozoneManagerStateMachine.updateLastAppliedIndex(flushedEpochs);

    Assert.assertEquals(0L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(10L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());
  }
}
