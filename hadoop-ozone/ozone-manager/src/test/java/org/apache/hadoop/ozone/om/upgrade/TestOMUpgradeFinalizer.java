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

package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeException.ResultCodes.LAYOUT_FEATURE_FINALIZATION_FAILED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link OMUpgradeFinalizer} tests.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestOMUpgradeFinalizer {

  private static final String CLIENT_ID = "clientID";
  private static final String OTHER_CLIENT_ID = "otherClientID";

  @Mock
  private OMLayoutVersionManager versionManager;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testEmitsFinalizedStatusIfAlreadyFinalized() throws Exception {

    when(versionManager.getUpgradeState()).thenReturn(ALREADY_FINALIZED);
    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    StatusAndMessages ret = finalizer.finalize(CLIENT_ID, null);

    assertEquals(ALREADY_FINALIZED, ret.status());
  }

  @Test
  public void testEmitsStartingStatusOnFinalization() throws Exception {
    Iterable<OMLayoutFeature> lfs = mockFeatures(3, "feature-3", "feature-4");
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    StatusAndMessages ret = finalizer.finalize(CLIENT_ID, mockOzoneManager(2));

    assertEquals(STARTING_FINALIZATION, ret.status());
  }

  /*
   * This test ensures that whenever finalize() is called, we finish all
   * finalization step, and getting the report gives back a FINALIZATION_DONE
   * status. This has to be revisited as soon as we change the behaviour to
   * post the finalization steps to the state machine from bg thread one by one.
   * This also means that FINALIZATION_IN_PROGRESS status related tests
   * has to be added at the same time.
   */
  @Test
  public void testReportStatusResultsInFinalizationDone()
      throws Exception {
    Iterable<OMLayoutFeature> lfs = mockFeatures(3, "feature-3", "feature-4");
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    finalizer.finalize(CLIENT_ID, mockOzoneManager(2));


    if (finalizer.isFinalizationDone()) {
      when(versionManager.getUpgradeState()).thenReturn(FINALIZATION_DONE);
    }
    StatusAndMessages ret = finalizer.reportStatus(CLIENT_ID, false);

    assertEquals(UpgradeFinalizer.Status.FINALIZATION_DONE, ret.status());
  }

  @Test
  public void testReportStatusAllowsTakeover()
      throws Exception {
    Iterable<OMLayoutFeature> lfs = mockFeatures(3, "feature-3", "feature-4");
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    finalizer.finalize(CLIENT_ID, mockOzoneManager(2));

    if (finalizer.isFinalizationDone()) {
      when(versionManager.getUpgradeState()).thenReturn(FINALIZATION_DONE);
    }
    StatusAndMessages ret = finalizer.reportStatus(OTHER_CLIENT_ID, true);

    assertEquals(UpgradeFinalizer.Status.FINALIZATION_DONE, ret.status());
  }

  @Test
  public void testReportStatusFailsFromNewClientIfRequestIsNotATakeover()
      throws Exception {
    Iterable<OMLayoutFeature> lfs = mockFeatures(3, "feature-3", "feature-4");
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    finalizer.finalize(CLIENT_ID, mockOzoneManager(2));

    exception.expect(UpgradeException.class);
    exception.expectMessage("Unknown client");

    finalizer.reportStatus(OTHER_CLIENT_ID, false);
  }

  @Test
  public void testFinalizationWithUpgradeAction() throws Exception {
    Optional<OmUpgradeAction> action = Optional.of(om -> om.getVersion());
    OzoneManager om = mockOzoneManager(0);
    Iterable<OMLayoutFeature> lfs = mockFeatures("feature-1", "feature-2");
    when(lfs.iterator().next().action(ON_FINALIZE)).thenReturn(action);
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    finalizer.finalize(CLIENT_ID, om);

    Iterator<OMLayoutFeature> it = lfs.iterator();
    OMLayoutFeature f = it.next();

    // the first feature has an upgrade action, and the action execution is
    // checked by verifying on om.getVersion
    verify(om.getOmStorage(), once())
        .setLayoutVersion(f.layoutVersion());
    verify(om, once()).getVersion();

    // The second feature has a NOOP, but should update the layout version.
    f = it.next();
    verify(om.getOmStorage(), once())
        .setLayoutVersion(f.layoutVersion());

    if (finalizer.isFinalizationDone()) {
      when(versionManager.getUpgradeState()).thenReturn(FINALIZATION_DONE);
    }
    StatusAndMessages status = finalizer.reportStatus(CLIENT_ID, false);
    assertEquals(FINALIZATION_DONE, status.status());
    assertFalse(status.msgs().isEmpty());
  }

  @Test
  public void testFinalizationWithFailingUpgradeAction() throws Exception {
    Optional<OmUpgradeAction> action = Optional.of(
        ignore -> {
          throw new IOException("Fail.");
        }
    );

    OzoneManager om = mockOzoneManager(0);
    Iterable<OMLayoutFeature> lfs = mockFeatures("feature-1", "feature-2");
    when(lfs.iterator().next().action(ON_FINALIZE)).thenReturn(action);
    setupVersionManagerMockToFinalize(lfs);

    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(versionManager);
    try {
      finalizer.finalize(CLIENT_ID, om);
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(UpgradeException.class));
      assertThat(e.getMessage(), containsString(lfs.iterator().next().name()));
      assertEquals(
          ((UpgradeException) e).getResult(),
          LAYOUT_FEATURE_FINALIZATION_FAILED
      );
    }
    if (finalizer.isFinalizationDone()) {
      when(versionManager.getUpgradeState()).thenReturn(FINALIZATION_DONE);
    }

    // Verify that we have never updated the layout version.
    Iterator<OMLayoutFeature> it = lfs.iterator();
    OMLayoutFeature f = it.next();
    verify(om.getOmStorage(), never())
        .setLayoutVersion(f.layoutVersion());

    // Verify that we never got to the second feature.
    f = it.next();
    verify(om.getOmStorage(), never())
        .setLayoutVersion(f.layoutVersion());

    StatusAndMessages status = finalizer.reportStatus(CLIENT_ID, false);
    assertEquals(FINALIZATION_DONE, status.status());
    assertFalse(status.msgs().isEmpty());
  }



  private VerificationMode once() {
    return times(1);
  }

  private void setupVersionManagerMockToFinalize(
      Iterable<? extends LayoutFeature> lfs
  ) {
    when(versionManager.getUpgradeState()).thenReturn(FINALIZATION_REQUIRED);
    when(versionManager.needsFinalization()).thenReturn(true);
    List<LayoutFeature> lfIter = new ArrayList<>();
    lfs.forEach(lfIter::add);
    when(versionManager.unfinalizedFeatures()).thenReturn(lfIter);
  }

  private OMLayoutFeature mockFeature(String name, int version) {
    OMLayoutFeature f = mock(OMLayoutFeature.class);
    when(f.name()).thenReturn(name);
    when(f.layoutVersion()).thenReturn(version);
    return f;
  }

  private Iterable<OMLayoutFeature> mockFeatures(String... names) {
    return mockFeatures(1, names);
  }

  private Iterable<OMLayoutFeature> mockFeatures(
      int startFromLV, String... names
  ) {
    int i = startFromLV;
    List<OMLayoutFeature> ret = new ArrayList<>();
    for (String name : names) {
      ret.add(mockFeature(name, i));
      i++;
    }
    return ret;
  }

  private int storedLayoutVersion = 0;

  private OzoneManager mockOzoneManager(int initialLayoutVersion) {
    OzoneManager mock = mock(OzoneManager.class);
    OMStorage st = mock(OMStorage.class);
    storedLayoutVersion = initialLayoutVersion;

    doAnswer(
        (Answer<Void>) inv -> {
          storedLayoutVersion = inv.getArgument(0, Integer.class);
          return null;
        }).when(st).setLayoutVersion(anyInt());

    when(st.getLayoutVersion())
        .thenAnswer((Answer<Integer>) ignore -> storedLayoutVersion);

    when(mock.getOmStorage()).thenReturn(st);

    return mock;
  }
}
