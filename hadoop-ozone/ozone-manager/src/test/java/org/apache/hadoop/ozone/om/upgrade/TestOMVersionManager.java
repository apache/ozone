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
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.ERASURE_CODING;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManagerImpl.getRequestClasses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.UnsupportedMockNewOMRequest;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test OM layout version management.
 */
public class TestOMVersionManager {

  @After
  public void cleanup() {
    OMLayoutVersionManagerImpl.resetLayoutVersionManager();
  }

  @Test
  public void testOMLayoutVersionManager() throws IOException {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(0);
    OMLayoutVersionManagerImpl omVersionManager =
        OMLayoutVersionManagerImpl.initialize(omStorage);
    OzoneManager om = mock(OzoneManager.class);
    when(om.getOmStorage()).thenReturn(omStorage);

    assertTrue(omVersionManager.isAllowed(INITIAL_VERSION));
    assertFalse(omVersionManager.isAllowed(ERASURE_CODING));
    assertEquals(INITIAL_VERSION.layoutVersion(),
        omVersionManager.getMetadataLayoutVersion());
    assertTrue(omVersionManager.needsFinalization());
    OMUpgradeFinalizer finalizer = new OMUpgradeFinalizer(omVersionManager);
    finalizer.finalize("random", om);
    assertFalse(omVersionManager.needsFinalization());
    assertEquals(ERASURE_CODING.layoutVersion(),
        omVersionManager.getMetadataLayoutVersion());
  }

  @Test
  public void testOMLayoutVersionManagerInitError() {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(
        OMLayoutFeature.values()[OMLayoutFeature.values().length - 1]
            .layoutVersion() + 1);
    try {
      OMLayoutVersionManagerImpl.initialize(omStorage);
      Assert.fail();
    } catch (OMException ex) {
      assertEquals(NOT_SUPPORTED_OPERATION, ex.getResult());
    }
  }

  @Test
  public void testOMLayoutVersionManagerReset() throws IOException {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(0);
    OMLayoutVersionManagerImpl omVersionManager =
        OMLayoutVersionManagerImpl.initialize(omStorage);
    int numLayoutVersions = OMLayoutFeature.values().length;
    assertEquals(
        OMLayoutFeature.values()[numLayoutVersions - 1].layoutVersion(),
        omVersionManager.getSoftwareLayoutVersion());
    OMLayoutVersionManagerImpl.resetLayoutVersionManager();
    assertEquals(0, omVersionManager.getSoftwareLayoutVersion());
  }

  @Test
  public void testOMLayoutFeatureCatalog() {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    int currVersion = Integer.MIN_VALUE;
    for (OMLayoutFeature lf : values) {
      assertTrue(currVersion <= lf.layoutVersion());
      currVersion = lf.layoutVersion();
    }
  }

  @Test
  public void testAllOMRequestClassesHaveRequestType()
      throws InvocationTargetException, IllegalAccessException {

    Set<Class<? extends OMClientRequest>> requestClasses =
        getRequestClasses();
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
  public void testCannotGetUnsupportedOmRequest() throws OMException {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(0);
    OMLayoutVersionManagerImpl omVersionManager =
        OMLayoutVersionManagerImpl.initialize(omStorage);
    OzoneManager om = mock(OzoneManager.class);
    when(om.getOmStorage()).thenReturn(omStorage);

    Class<? extends OMClientRequest> requestHandler;
    try {
      requestHandler = omVersionManager.getRequestHandler(
              UnsupportedMockNewOMRequest.class.getSimpleName());
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains(
          "No suitable instance found", ex);
    }

    omVersionManager.unfinalizedFeatures().forEach(omVersionManager::finalized);
    requestHandler = omVersionManager.getRequestHandler(
        UnsupportedMockNewOMRequest.class.getSimpleName());
    Assert.assertNotNull(requestHandler);
  }
}