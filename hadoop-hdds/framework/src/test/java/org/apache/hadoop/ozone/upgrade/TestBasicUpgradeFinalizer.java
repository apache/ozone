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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_1;
import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_3;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_DONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for BasicUpgradeFinalizer.
 */
public class TestBasicUpgradeFinalizer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBasicUpgradeFinalizer.class);

  @Test
  public void testFinalizerPhasesAreInvokedInOrder() throws IOException {
    SimpleTestFinalizer finalizer = spy(SimpleTestFinalizer.class);
    InOrder inOrder = inOrder(finalizer);

    Object mockObj = mock(Object.class);
    doCallRealMethod().when(finalizer).finalize(anyString(),
        eq(mockObj));

    finalizer.finalize("test-client-1", mockObj);

    StatusAndMessages res = finalizer.reportStatus("test-client-1", false);
    assertEquals(FINALIZATION_DONE, res.status());

    inOrder.verify(finalizer).preFinalizeUpgrade(eq(mockObj));
    inOrder.verify(finalizer).finalizeLayoutFeature(
        eq(
            TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_2),
        eq(mockObj));
    inOrder.verify(finalizer).finalizeLayoutFeature(
        eq(
            TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_3),
        eq(mockObj));
    inOrder.verify(finalizer).postFinalizeUpgrade(eq(mockObj));

    assertTrue(finalizer.isFinalizationDone());
    assertTrue(finalizer.preCalled && finalizer.finalizeCalled &&
        finalizer.postCalled);
  }

  @Test
  public void testAlreadyFinalizedDoesNotTriggerNewFinalization()
      throws IOException {
    SimpleTestFinalizer finalizer = new SimpleTestFinalizer(
        new MockLayoutVersionManager(VERSION_3.layoutVersion()));

    Object mockObj = mock(Object.class);
    StatusAndMessages res =
        finalizer.finalize("test-client-1", mockObj);

    assertEquals(ALREADY_FINALIZED, res.status());
    assertFalse(finalizer.preCalled || finalizer.finalizeCalled ||
        finalizer.postCalled);
  }


  /**
   * Tests that the upgrade finalizer gives expected statuses when multiple
   * clients invoke finalize and query finalize status simultaneously.
   * @throws Exception
   */
  @Test
  public void testConcurrentFinalization() throws Exception {
    CountDownLatch pauseLatch = new CountDownLatch(1);
    CountDownLatch unpauseLatch = new CountDownLatch(1);
    // Pause finalization to test concurrent finalize requests. The injection
    // point to pause at does not matter.
    InjectedUpgradeFinalizationExecutor<Object> executor =
        UpgradeTestUtils.newPausingFinalizationExecutor(
            UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE,
            pauseLatch, unpauseLatch, LOG);
    SimpleTestFinalizer finalizer =
        new SimpleTestFinalizer(
            new MockLayoutVersionManager(VERSION_1.layoutVersion()), executor);

    // The first finalize call should block until the executor is unpaused.
    Future<?> firstFinalizeFuture = runFinalization(finalizer,
        UpgradeFinalization.Status.STARTING_FINALIZATION);
    // Wait for finalization to pause at the halting point.
    pauseLatch.await();

    Future<?> secondFinalizeFuture = runFinalization(finalizer,
        UpgradeFinalization.Status.FINALIZATION_IN_PROGRESS);
    Future<?> finalizeQueryFuture = runFinalizationQuery(finalizer,
        UpgradeFinalization.Status.FINALIZATION_IN_PROGRESS);

    // While finalization is paused, the two following requests should have
    // reported it is in progress.
    secondFinalizeFuture.get();
    finalizeQueryFuture.get();

    // Now resume finalization so the initial finalize request can complete.
    unpauseLatch.countDown();
    firstFinalizeFuture.get();

    // All subsequent queries should return finalization done, even if they
    // land in parallel.
    List<Future<?>> finalizeFutures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      finalizeFutures.add(runFinalizationQuery(finalizer,
          UpgradeFinalization.Status.FINALIZATION_DONE));
    }

    // Wait for all queries to complete.
    for (Future<?> finalizeFuture: finalizeFutures) {
      finalizeFuture.get();
    }
  }

  private Future<?> runFinalization(
      BasicUpgradeFinalizer<Object, MockLayoutVersionManager> finalizer,
      UpgradeFinalization.Status expectedStatus) {
    return Executors.newSingleThreadExecutor().submit(() -> {
      try {
        StatusAndMessages result = finalizer.finalize("test", new Object());
        assertEquals(expectedStatus, result.status());
      } catch (Exception ex) {
        LOG.error("Finalization failed", ex);
        fail("Finalization failed with exception: " +
            ex.getMessage());
      }
    });
  }

  private Future<?> runFinalizationQuery(UpgradeFinalizer<Object> finalizer,
      UpgradeFinalization.Status expectedStatus) {
    return Executors.newSingleThreadExecutor().submit(() -> {
      assertEquals(expectedStatus, finalizer.getStatus());
    });
  }

  /**
   * Yet another mock finalizer.
   */
  static class SimpleTestFinalizer extends BasicUpgradeFinalizer<Object,
      MockLayoutVersionManager> {

    private boolean preCalled = false;
    private boolean finalizeCalled = false;
    private boolean postCalled = false;

    /**
     * Invoked by Mockito.
     */
    SimpleTestFinalizer() throws IOException {
      super(new MockLayoutVersionManager(VERSION_1.layoutVersion()));
    }

    SimpleTestFinalizer(MockLayoutVersionManager lvm) {
      super(lvm);
    }

    SimpleTestFinalizer(MockLayoutVersionManager lvm,
                        UpgradeFinalizationExecutor<Object> executor) {
      super(lvm, executor);
    }

    @Override
    protected void preFinalizeUpgrade(Object service) throws IOException {
      super.preFinalizeUpgrade(service);
      preCalled = true;
    }

    @Override
    protected void postFinalizeUpgrade(Object service) throws IOException {
      super.postFinalizeUpgrade(service);
      postCalled = true;
    }

    @Override
    public void finalizeLayoutFeature(LayoutFeature lf, Object service)
        throws UpgradeException {
      Storage mockStorage = mock(Storage.class);
      InOrder inOrder = inOrder(mockStorage);

      super.finalizeLayoutFeature(lf,
          lf.action(LayoutFeature.UpgradeActionType.ON_FINALIZE), mockStorage);

      inOrder.verify(mockStorage).setLayoutVersion(eq(lf.layoutVersion()));
      try {
        inOrder.verify(mockStorage).persistCurrentState();
      } catch (IOException ex) {
        throw new UpgradeException(ex,
            UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED);
      }
      finalizeCalled = true;
    }

    @Override
    public void runPrefinalizeStateActions(Storage storage, Object service) {
      // no-op for testing.
    }
  }
}
