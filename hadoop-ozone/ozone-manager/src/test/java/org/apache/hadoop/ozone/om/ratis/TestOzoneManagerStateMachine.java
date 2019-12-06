package org.apache.hadoop.ozone.om.ratis;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * Class to test OzoneManagerStateMachine.
 */
public class TestOzoneManagerStateMachine {

  private OzoneManagerStateMachine ozoneManagerStateMachine;
  @Before
  public void setup() {
    OzoneManagerRatisServer ozoneManagerRatisServer =
        Mockito.mock(OzoneManagerRatisServer.class);
    OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);

    when(ozoneManagerRatisServer.getOzoneManager()).thenReturn(ozoneManager);
    when(ozoneManager.getSnapshotInfo()).thenReturn(Mockito.mock(OMRatisSnapshotInfo.class));
    ozoneManagerStateMachine =
        new OzoneManagerStateMachine(ozoneManagerRatisServer);
  }

  @Test
  public void testLastAppliedIndex() {

    // Happy scenario.

    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyIndexUpdate(0, 1);
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
    ozoneManagerStateMachine.notifyIndexUpdate(0L, 4L);

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
    ozoneManagerStateMachine.notifyIndexUpdate(0, 1);
    Assert.assertEquals(0,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getTerm());
    Assert.assertEquals(1,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());



    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 2L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 3L);
    ozoneManagerStateMachine.addApplyTransactionTermIndex(0L, 4L);



    // Conf/metadata transaction.
    ozoneManagerStateMachine.notifyIndexUpdate(0L, 5L);

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
    Assert.assertEquals(1L,
        ozoneManagerStateMachine.getLastAppliedTermIndex().getIndex());

  }
}
