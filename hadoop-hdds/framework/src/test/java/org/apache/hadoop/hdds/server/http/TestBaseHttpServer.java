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

package org.apache.hadoop.hdds.server.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test Common ozone/hdds web methods.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestBaseHttpServer {

  private static final String ADDRESS_HTTP_KEY = "address.http";
  private static final String ADDRESS_HTTPS_KEY = "address.https";
  private static final String BIND_HOST_HTTP_KEY = "bind-host.http";
  private static final String BIND_HOST_HTTPS_KEY = "bind-host.https";
  private static final String BIND_HOST_DEFAULT = "0.0.0.0";
  private static final int BIND_PORT_HTTP_DEFAULT = PortAllocator.getFreePort();
  private static final int BIND_PORT_HTTPS_DEFAULT = PortAllocator.getFreePort();
  private static final String ENABLED_KEY = "enabled";

  private String hostname;

  @TempDir
  private Path tempDir;

  @BeforeAll
  void setup() throws Exception {
    hostname = InetAddress.getLocalHost().getHostName();
  }

  @Test
  public void getBindAddress() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("enabled", "false");

    BaseHttpServer baseHttpServer = new BaseHttpServer(conf, "test") {
      @Override
      protected String getHttpAddressKey() {
        return null;
      }

      @Override
      protected String getHttpsAddressKey() {
        return null;
      }

      @Override
      protected String getHttpBindHostKey() {
        return null;
      }

      @Override
      protected String getHttpsBindHostKey() {
        return null;
      }

      @Override
      protected String getBindHostDefault() {
        return null;
      }

      @Override
      protected int getHttpBindPortDefault() {
        return 0;
      }

      @Override
      protected int getHttpsBindPortDefault() {
        return 0;
      }

      @Override
      protected String getKeytabFile() {
        return null;
      }

      @Override
      protected String getSpnegoPrincipal() {
        return null;
      }

      @Override
      protected String getEnabledKey() {
        return "enabled";
      }

      @Override
      protected String getHttpAuthType() {
        return "simple";
      }

      @Override
      protected String getHttpAuthConfigPrefix() {
        return null;
      }
    };

    conf.set("addresskey", "0.0.0.0:1234");

    assertEquals("/0.0.0.0:1234", baseHttpServer
        .getBindAddress("bindhostkey", "addresskey",
            "default", 65).toString());

    conf.set("bindhostkey", "1.2.3.4");

    assertEquals("/1.2.3.4:1234", baseHttpServer
        .getBindAddress("bindhostkey", "addresskey",
            "default", 65).toString());
  }

  @ParameterizedTest
  @EnumSource
  void updatesAddressInConfig(HttpConfig.Policy policy) throws Exception {
    MutableConfigurationSource conf = newConfig(policy);

    BaseHttpServer subject = new TestingHttpServer(conf);

    try {
      subject.start();

      if (policy.isHttpEnabled()) {
        assertEquals(hostname + ":" + subject.getHttpAddress().getPort(), conf.get(ADDRESS_HTTP_KEY));
      }
      if (policy.isHttpsEnabled()) {
        assertEquals(hostname + ":" + subject.getHttpsAddress().getPort(), conf.get(ADDRESS_HTTPS_KEY));
      }
    } finally {
      subject.stop();
    }
  }

  private MutableConfigurationSource newConfig(HttpConfig.Policy policy) {
    MutableConfigurationSource conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_BASEDIR, tempDir.toString());
    conf.setEnum(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy);
    return conf;
  }

  private static class TestingHttpServer extends BaseHttpServer {

    TestingHttpServer(MutableConfigurationSource conf) throws IOException {
      super(conf, "testing");
    }

    @Override
    protected String getHttpAddressKey() {
      return ADDRESS_HTTP_KEY;
    }

    @Override
    protected String getHttpsAddressKey() {
      return ADDRESS_HTTPS_KEY;
    }

    @Override
    protected String getHttpBindHostKey() {
      return BIND_HOST_HTTP_KEY;
    }

    @Override
    protected String getHttpsBindHostKey() {
      return BIND_HOST_HTTPS_KEY;
    }

    @Override
    protected String getBindHostDefault() {
      return BIND_HOST_DEFAULT;
    }

    @Override
    protected int getHttpBindPortDefault() {
      return BIND_PORT_HTTP_DEFAULT;
    }

    @Override
    protected int getHttpsBindPortDefault() {
      return BIND_PORT_HTTPS_DEFAULT;
    }

    @Override
    protected String getKeytabFile() {
      throw new NotImplementedException();
    }

    @Override
    protected String getSpnegoPrincipal() {
      throw new NotImplementedException();
    }

    @Override
    protected String getEnabledKey() {
      return ENABLED_KEY;
    }

    @Override
    protected String getHttpAuthType() {
      throw new NotImplementedException();
    }

    @Override
    protected String getHttpAuthConfigPrefix() {
      throw new NotImplementedException();
    }
  }

}
