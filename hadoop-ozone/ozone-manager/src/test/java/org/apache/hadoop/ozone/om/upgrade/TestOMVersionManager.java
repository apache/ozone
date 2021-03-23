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
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.getRequestClasses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.junit.Assert;
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
}