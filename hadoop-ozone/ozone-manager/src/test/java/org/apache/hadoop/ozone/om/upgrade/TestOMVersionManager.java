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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.OM_UPGRADE_CLASS_PACKAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.junit.jupiter.api.Test;

/**
 * Test OM layout version management.
 */
public class TestOMVersionManager {

  @Test
  public void testOMLayoutVersionManager() throws IOException {
    OMLayoutVersionManager omVersionManager =
        new OMLayoutVersionManager();

    // Initial Version is always allowed.
    assertTrue(omVersionManager.isAllowed(INITIAL_VERSION));
    assertThat(INITIAL_VERSION.layoutVersion())
        .isLessThanOrEqualTo(omVersionManager.getMetadataLayoutVersion());
  }

  @Test
  public void testOMLayoutVersionManagerInitError() {
    int lV = OMLayoutFeature.values()[OMLayoutFeature.values().length - 1]
        .layoutVersion() + 1;
    OMException ome = assertThrows(OMException.class, () -> new OMLayoutVersionManager(lV));
    assertEquals(NOT_SUPPORTED_OPERATION, ome.getResult());
  }

  @Test
  public void testOMLayoutFeaturesHaveIncreasingLayoutVersion()
      throws Exception {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    int currVersion = -1;
    OMLayoutFeature lastFeature = null;
    for (OMLayoutFeature lf : values) {
      assertEquals(currVersion + 1, lf.layoutVersion());
      currVersion = lf.layoutVersion();
      lastFeature = lf;
    }
    lastFeature.addAction(arg -> {
      String v = arg.getVersion();
    });

    OzoneManager omMock = mock(OzoneManager.class);
    lastFeature.action().get().execute(omMock);
    verify(omMock, times(1)).getVersion();
  }

  @Test

  /*
   * The OMLayoutFeatureAspect relies on the fact that the OM client
   * request handler class has a preExecute method with first argument as
   * 'OzoneManager'. If that is not true, please fix
   * OMLayoutFeatureAspect#beforeRequestApplyTxn.
   */
  public void testOmClientRequestPreExecuteIsCompatibleWithAspect() {
    Method[] methods = OMClientRequest.class.getMethods();

    Optional<Method> preExecuteMethod = Arrays.stream(methods)
            .filter(m -> m.getName().equals("preExecute"))
            .findFirst();

    assertTrue(preExecuteMethod.isPresent());
    assertThat(preExecuteMethod.get().getParameterCount()).isGreaterThanOrEqualTo(1);
    assertEquals(OzoneManager.class,
        preExecuteMethod.get().getParameterTypes()[0]);
  }

  @Test
  public void testOmUpgradeActionsRegistered() throws Exception {
    OMLayoutVersionManager lvm = new OMLayoutVersionManager(); // MLV >= 0
    assertFalse(lvm.needsFinalization());

    // INITIAL_VERSION is finalized, hence should not register.
    Optional<OmUpgradeAction> action =
        INITIAL_VERSION.action();
    assertFalse(action.isPresent());

    lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(-1);
    doCallRealMethod().when(lvm).registerUpgradeActions(anyString());
    lvm.registerUpgradeActions(OM_UPGRADE_CLASS_PACKAGE);

    action = INITIAL_VERSION.action();
    assertTrue(action.isPresent());
    assertEquals(MockOmUpgradeAction.class, action.get().getClass());
    OzoneManager omMock = mock(OzoneManager.class);
    action.get().execute(omMock);
    verify(omMock, times(1)).getVersion();
  }

  /**
   * Mock OM upgrade action class.
   */
  @UpgradeActionOm(feature =
      INITIAL_VERSION)
  public static class MockOmUpgradeAction implements OmUpgradeAction {

    @Override
    public void execute(OzoneManager arg) {
      arg.getVersion();
    }
  }
}
