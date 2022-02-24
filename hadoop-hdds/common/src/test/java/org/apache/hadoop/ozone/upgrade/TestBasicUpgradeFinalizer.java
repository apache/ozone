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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_1;
import static org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutFeature.VERSION_3;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.upgrade.TestUpgradeFinalizerActions.MockLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;

/**
 * Test for BasicUpgradeFinalizer.
 */
public class TestBasicUpgradeFinalizer {

  @Test
  public void testFinalizerPhasesAreInvokedInOrder() throws IOException {
    SimpleTestFinalizer finalizer = spy(SimpleTestFinalizer.class);
    InOrder inOrder = inOrder(finalizer);

    Object mockObj = mock(Object.class);
    doCallRealMethod().when(finalizer).finalize(anyString(),
        ArgumentMatchers.eq(mockObj));

    finalizer.finalize("test-client-1", mockObj);

    StatusAndMessages res = finalizer.reportStatus("test-client-1", false);
    assertEquals(FINALIZATION_DONE, res.status());

    inOrder.verify(finalizer).preFinalizeUpgrade(ArgumentMatchers.eq(mockObj));
    inOrder.verify(finalizer).finalizeUpgrade(ArgumentMatchers.eq(mockObj));
    inOrder.verify(finalizer).postFinalizeUpgrade(ArgumentMatchers.eq(mockObj));

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
   * Yet another mock finalizer.
   */
  static class SimpleTestFinalizer extends BasicUpgradeFinalizer<Object,
      MockLayoutVersionManager> {

    private boolean preCalled = false;
    private boolean finalizeCalled = false;
    private boolean postCalled = false;

    SimpleTestFinalizer() throws IOException {
      super(new MockLayoutVersionManager(VERSION_1.layoutVersion()));
    }

    SimpleTestFinalizer(MockLayoutVersionManager lvm) {
      super(lvm);
    }

    @Override
    protected void preFinalizeUpgrade(Object service) {
      preCalled = true;
    }

    @Override
    protected void postFinalizeUpgrade(Object service) {
      postCalled = true;
    }

    @Override
    public void finalizeUpgrade(Object service) {
      finalizeCalled = true;
      getVersionManager().completeFinalization();
    }

    @Override
    public void runPrefinalizeStateActions(Storage storage, Object service) {

    }
  }
}
