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

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

/**
 * Test OmVersionFactory.
 */
@Unhealthy("Ignored since this is incompatible" +
    " with HDDS-2939 work. Potentially revisit later.")
public class TestOmVersionManagerRequestFactory {

  private static OMLayoutVersionManager omVersionManager;

  @BeforeAll
  public static void setup() throws OMException {
    omVersionManager = new OMLayoutVersionManager(0);
  }

  @Test
  public void testKeyCreateRequest() throws Exception {

    // Try getting v1 of 'CreateKey'.
    Class<? extends OMClientRequest> requestType =
        omVersionManager.getHandler(CreateKey.name());
    assertEquals(requestType, OMKeyCreateRequest.class);
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

      Constructor<? extends OMClientRequest> constructorWithOmRequestArg =
          requestClass.getDeclaredConstructor(OMRequest.class);
      assertNotNull(constructorWithOmRequestArg);
    }
  }
}
