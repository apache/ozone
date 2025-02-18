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

package org.apache.hadoop.ozone.om.protocolPB;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.ServiceLoader;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Test OmTransportFactory.
 */
public class TestOmTransportFactory {
  private OzoneConfiguration conf;

  @BeforeEach
  void setUp() {
    conf = new OzoneConfiguration();
  }

  @Test
  public void testCreateFactoryFromServiceLoader() throws IOException {
    OmTransportFactory dummyImpl = mock(OmTransportFactory.class);
    ServiceLoader<OmTransportFactory> serviceLoader = mock(ServiceLoader.class);
    when(serviceLoader.iterator()).thenReturn(singletonList(dummyImpl).iterator());

    try (MockedStatic mocked = mockStatic(ServiceLoader.class)) {
      mocked.when(() -> ServiceLoader.load(OmTransportFactory.class)).thenReturn(serviceLoader);

      OmTransportFactory factory = OmTransportFactory.createFactory(conf);
      assertEquals(dummyImpl, factory);
    }
  }

  @Test
  public void testCreateFactoryFromConfig() throws IOException {
    ServiceLoader<OmTransportFactory> emptyLoader = mock(ServiceLoader.class);
    when(emptyLoader.iterator()).thenReturn(Collections.emptyIterator());

    try (MockedStatic mocked = mockStatic(ServiceLoader.class)) {
      mocked.when(() -> ServiceLoader.load(OmTransportFactory.class))
          .thenReturn(emptyLoader);

      // Without anything in config, the default transport is returned.
      OmTransportFactory factory = OmTransportFactory.createFactory(conf);
      assertEquals(OZONE_OM_TRANSPORT_CLASS_DEFAULT, factory.getClass().getName());

      // With concrete class name indicated in config.
      conf.set(OZONE_OM_TRANSPORT_CLASS, MyDummyTransport.class.getName());
      OmTransportFactory factory2 = OmTransportFactory.createFactory(conf);
      assertEquals(MyDummyTransport.class, factory2.getClass());

      // With non-existing class name in the config, exception is expected.
      conf.set(OZONE_OM_TRANSPORT_CLASS, "com.MyMadeUpClass");
      assertThrows(IOException.class, () -> {
        OmTransportFactory.createFactory(conf);
      });
    }
  }

  static class MyDummyTransport implements OmTransportFactory {

    @Override
    public OmTransport createOmTransport(ConfigurationSource source,
        UserGroupInformation ugi, String omServiceId) throws IOException {
      return null;
    }
  }
}
