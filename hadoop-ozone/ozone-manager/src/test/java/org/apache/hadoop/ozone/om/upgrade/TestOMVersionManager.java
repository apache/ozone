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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.OM_REQUEST_CLASS_PACKAGE;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.OM_UPGRADE_CLASS_PACKAGE;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.getRequestClasses;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.VALIDATE_IN_PREFINALIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
    assertTrue(INITIAL_VERSION.layoutVersion() <=
        omVersionManager.getMetadataLayoutVersion());
  }

  @Test
  public void testOMLayoutVersionManagerInitError() {
    int lV = OMLayoutFeature.values()[OMLayoutFeature.values().length - 1]
        .layoutVersion() + 1;

    try {
      new OMLayoutVersionManager(lV);
      Assert.fail();
    } catch (OMException ex) {
      assertEquals(NOT_SUPPORTED_OPERATION, ex.getResult());
    }
  }

  @Test
  public void testOMLayoutFeaturesHaveIncreasingLayoutVersion()
      throws Exception {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    int currVersion = Integer.MIN_VALUE;
    OMLayoutFeature lastFeature = null;
    for (OMLayoutFeature lf : values) {
      assertTrue(currVersion < lf.layoutVersion());
      currVersion = lf.layoutVersion();
      lastFeature = lf;
    }
    for (UpgradeActionType type : UpgradeActionType.values()) {
      lastFeature.addAction(type, arg -> {
        String v = arg.getVersion();
      });
    }

    OzoneManager omMock = mock(OzoneManager.class);
    for (UpgradeActionType type : UpgradeActionType.values()) {
      lastFeature.action(type).get().execute(omMock);
    }
    verify(omMock, times(UpgradeActionType.values().length)).getVersion();
  }

  @Ignore("Since there is no longer a need to enforce the getRequestType " +
      "method in OM request classes, disabling the " +
      "test. Potentially revisit later.")
  @Test
  public void testAllOMRequestClassesHaveRequestType()
      throws InvocationTargetException, IllegalAccessException {

    Set<Class<? extends OMClientRequest>> requestClasses =
        getRequestClasses(OM_REQUEST_CLASS_PACKAGE);
    Set<String> requestTypes = new HashSet<>();

    for (Class<? extends OMClientRequest> requestClass : requestClasses) {
      try {
        Method getRequestTypeMethod = requestClass.getMethod(
            "getRequestType");
        String type = (String) getRequestTypeMethod.invoke(null);

        int lVersion = INITIAL_VERSION.layoutVersion();
        BelongsToLayoutVersion annotation =
            requestClass.getAnnotation(BelongsToLayoutVersion.class);
        if (annotation != null) {
          lVersion = annotation.value().layoutVersion();
        }
        if (requestTypes.contains(type + "-" + lVersion)) {
          Assert.fail("Duplicate request/version type found : " + type);
        }
        requestTypes.add(type + "-" + lVersion);
      } catch (NoSuchMethodException nsmEx) {
        Assert.fail("getRequestType method not defined in a class." +
            nsmEx.getMessage());
      }
    }
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
    assertTrue(preExecuteMethod.get().getParameterCount() >= 1);
    assertEquals(OzoneManager.class,
        preExecuteMethod.get().getParameterTypes()[0]);
  }

  @Test
  public void testOmUpgradeActionsRegistered() throws Exception {
    OMLayoutVersionManager lvm = new OMLayoutVersionManager(); // MLV >= 0
    assertFalse(lvm.needsFinalization());

    // INITIAL_VERSION is finalized, hence should not register.
    Optional<OmUpgradeAction> action =
        INITIAL_VERSION.action(VALIDATE_IN_PREFINALIZE);
    assertFalse(action.isPresent());

    lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(-1);
    doCallRealMethod().when(lvm).registerUpgradeActions(anyString());
    lvm.registerUpgradeActions(OM_UPGRADE_CLASS_PACKAGE);

    action = INITIAL_VERSION.action(VALIDATE_IN_PREFINALIZE);
    Assert.assertTrue(action.isPresent());
    Assert.assertEquals(MockOmUpgradeAction.class, action.get().getClass());
    OzoneManager omMock = mock(OzoneManager.class);
    action.get().execute(omMock);
    verify(omMock, times(1)).getVersion();
  }

  /**
   * Mock OM upgrade action class.
   */
  @UpgradeActionOm(type = VALIDATE_IN_PREFINALIZE, feature =
      INITIAL_VERSION)
  public static class MockOmUpgradeAction implements OmUpgradeAction {

    @Override
    public void execute(OzoneManager arg) {
      arg.getVersion();
    }
  }
}