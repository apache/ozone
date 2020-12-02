/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.UnsupportedMockNewOMRequest;
import org.apache.hadoop.ozone.om.request.key.OMMockECKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reflections.Reflections;

/**
 * Test OmVersionFactory.
 */
public class TestOmVersionManagerRequestFactory {

  private static OMLayoutVersionManagerImpl omVersionManager;
  private static OzoneManager om;

  @BeforeClass
  public static void setup() throws OMException {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(0);
    omVersionManager = OMLayoutVersionManagerImpl.initialize(omStorage);
    om = mock(OzoneManager.class);
    when(om.getOmStorage()).thenReturn(omStorage);
  }

  @Test
  public void testKeyCreateRequest() throws Exception {

    // Try getting v1 of 'CreateKey'.
    Class<? extends OMClientRequest> requestType =
        omVersionManager.getRequestHandler(CreateKey.name());
    Assert.assertEquals(requestType, OMKeyCreateRequest.class);

    // Finalize the version manager.
    OMUpgradeFinalizer f = new OMUpgradeFinalizer(omVersionManager);
    f.finalize("random", om);

    // Try getting 'CreateKey' again. Should return CreateECKey.
    requestType = omVersionManager.getRequestHandler(CreateKey.name());
    Assert.assertEquals(requestType, OMMockECKeyCreateRequest.class);
  }

  @Test
  public void testAllOMRequestClassesHaveGetRequestTypeMethod()
      throws Exception {
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.request");
    Set<Class<? extends OMClientRequest>> subTypes =
        reflections.getSubTypesOf(OMClientRequest.class);
    List<Class<? extends OMClientRequest>> collect = subTypes.stream()
            .filter(c -> !Modifier.isAbstract(c.getModifiers()))
            .collect(Collectors.toList());

    for (Class<? extends OMClientRequest> c : collect) {
      if (c.equals(UnsupportedMockNewOMRequest.class)) {
        continue;
      }
      Method getRequestTypeMethod = null;
      try {
        getRequestTypeMethod = c.getMethod("getRequestType");
      } catch (NoSuchMethodException nsmEx) {
        Assert.fail(String.format(
            "%s does not have the 'getRequestType' method " +
            "which should be defined or inherited for every OM request class.",
            c));
      }
      String type = (String) getRequestTypeMethod.invoke(null);
      Assert.assertNotNull(String.format("Cannot get handler for %s", type),
          omVersionManager.getRequestHandler(type));
    }
  }

  @Test
  public void testOmClientRequestHasExpectedConstructor()
      throws NoSuchMethodException {
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.request");
    Set<Class<? extends OMClientRequest>> subTypes =
        reflections.getSubTypesOf(OMClientRequest.class);

    for (Class<? extends OMClientRequest> requestClass : subTypes) {
      if (Modifier.isAbstract(requestClass.getModifiers())) {
        continue;
      }
      Method getRequestTypeMethod = requestClass.getMethod(
          "getRequestType");
      Assert.assertNotNull(getRequestTypeMethod);

      Constructor<? extends OMClientRequest> constructorWithOmRequestArg =
          requestClass.getDeclaredConstructor(OMRequest.class);
      Assert.assertNotNull(constructorWithOmRequestArg);
    }
  }
}
