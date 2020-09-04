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

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.CREATE_EC;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeatureCatalog.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Modifier;
import java.util.Set;

import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.key.OMECKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reflections.Reflections;

/**
 * Test OmVersionFactory.
 */
public class TestOmRequestFactory {

  private static OmRequestFactory omRequestFactory;
  private static OMLayoutVersionManager omVersionManager;

  @BeforeClass
  public static void setup() throws OMException {
    OMStorage omStorage = mock(OMStorage.class);
    when(omStorage.getLayoutVersion()).thenReturn(0);
    omVersionManager = OMLayoutVersionManager.initialize(omStorage);
    omRequestFactory = omVersionManager.getVersionFactory();
  }

  @Test
  public void testKeyCreateRequest() {
    Class<? extends OMClientRequest> requestType =
        omRequestFactory.getRequestType(OMRequest.newBuilder()
            .setCmdType(CreateKey)
            .setClientId("c1")
            .setLayoutVersion(LayoutVersion
                .newBuilder()
                .setVersion(INITIAL_VERSION.layoutVersion())
                .build())
            .build());
    Assert.assertEquals(requestType, OMKeyCreateRequest.class);

    try {
      omRequestFactory.getRequestType(OMRequest.newBuilder()
              .setCmdType(CreateKey)
              .setClientId("c1")
              .setLayoutVersion(LayoutVersion
                  .newBuilder()
                  .setVersion(CREATE_EC.layoutVersion())
                  .build())
              .build());
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }

    omVersionManager.doFinalize(null);
    requestType = omRequestFactory.getRequestType(OMRequest.newBuilder()
        .setCmdType(CreateKey)
        .setClientId("c1")
        .setLayoutVersion(LayoutVersion
            .newBuilder()
            .setVersion(CREATE_EC.layoutVersion())
            .build())
        .build());
    Assert.assertEquals(requestType, OMECKeyCreateRequest.class);
  }

  @Test
  public void testAllOMRequestClassesRegistered() {
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.request");
    Set<Class<? extends OMClientRequest>> subTypes =
        reflections.getSubTypesOf(OMClientRequest.class);
    long count = subTypes.stream().filter(
        c -> !Modifier.isAbstract(c.getModifiers())).count();

    Integer countFromFactory = omRequestFactory.getRequestFactory()
        .getInstances().values().stream()
        .map(c -> c.size()).reduce(0, Integer::sum);
    Assert.assertEquals(Math.toIntExact(count), (int) countFromFactory);
  }
}