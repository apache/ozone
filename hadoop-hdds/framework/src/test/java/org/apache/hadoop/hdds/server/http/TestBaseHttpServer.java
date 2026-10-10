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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.webapp.WebAppContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  private static final String HTTP_AUTH_TYPE_KEY = "http.auth.type";
  private static final String HTTP_AUTH_CONFIG_PREFIX = "http.auth.";
  private static final String KEYTAB_KEY = "http.kerberos.keytab";
  private static final String SPNEGO_PRINCIPAL_KEY = "http.kerberos.principal";

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

    // An IPv6 bind host, wildcard or literal, must survive being combined with
    // the port. Assert on the address rather than toString(), whose bracketing
    // of IPv6 literals is a JDK detail.
    conf.set("bindhostkey", "::");

    InetSocketAddress wildcard = baseHttpServer
        .getBindAddress("bindhostkey", "addresskey", "default", 65);
    assertEquals(InetAddress.getByName("::"), wildcard.getAddress());
    assertEquals(1234, wildcard.getPort());

    conf.set("bindhostkey", "2001:db8::1");

    InetSocketAddress literal = baseHttpServer
        .getBindAddress("bindhostkey", "addresskey", "default", 65);
    assertEquals(InetAddress.getByName("2001:db8::1"), literal.getAddress());
    assertEquals(1234, literal.getPort());
  }

  @Test
  void endpointUriKeepsHostAndPort() {
    URI ipv4 = BaseHttpServer.newEndpointUri("http", new InetSocketAddress("192.0.2.1", 9874));
    assertEquals("192.0.2.1", ipv4.getHost());
    assertEquals(9874, ipv4.getPort());

    URI ipv6 = BaseHttpServer.newEndpointUri("https", new InetSocketAddress("2001:db8::1", 9875));
    assertEquals("[2001:db8:0:0:0:0:0:1]", ipv6.getHost());
    assertEquals(9875, ipv6.getPort());
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

  /**
   * Each server must use its own temp subdirectory (named after the server)
   * under the base directory, so multiple WebAppContexts in one process do not
   * share Jetty scratch space, and the subdirectory is kept persistent so Jetty
   * does not delete operator data under the metadata directory on stop.
   */
  @Test
  void usesPerServerTempSubdirectory() throws Exception {
    MutableConfigurationSource conf = newConfig(HttpConfig.Policy.HTTP_ONLY);
    BaseHttpServer subject = new TestingHttpServer(conf);
    try {
      subject.start();
      WebAppContext webAppContext = httpServer2Of(subject).getWebAppContext();
      assertEquals(new File(tempDir.toFile(), "testing").getCanonicalFile(),
          webAppContext.getTempDirectory().getCanonicalFile());
      assertTrue(webAppContext.isPersistTempDirectory());
    } finally {
      subject.stop();
    }
  }

  /**
   * An operator-configured {@code hadoop.http.temp.dir} is applied by
   * {@link HttpServer2} during build and must not be silently replaced by the
   * per-server {@code <basedir>/<name>} subdirectory, preserving the
   * pre-Jetty-12 precedence where the explicit setting wins.
   */
  @Test
  void honoursOperatorConfiguredTempDir() throws Exception {
    MutableConfigurationSource conf = newConfig(HttpConfig.Policy.HTTP_ONLY);
    File operatorTempDir = new File(tempDir.toFile(), "operator-temp");
    conf.set("hadoop.http.temp.dir", operatorTempDir.getAbsolutePath());
    BaseHttpServer subject = new TestingHttpServer(conf);
    try {
      subject.start();
      WebAppContext webAppContext = httpServer2Of(subject).getWebAppContext();
      assertEquals(operatorTempDir.getCanonicalFile(),
          webAppContext.getTempDirectory().getCanonicalFile());
    } finally {
      subject.stop();
    }
  }

  static Stream<Arguments> advertisedAddressesNamingNoHost() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of("0.0.0.0:9874", "0.0.0.0:9875"),
        Arguments.of("[::]:9874", "[::]:9875"));
  }

  @ParameterizedTest
  @MethodSource("advertisedAddressesNamingNoHost")
  void spnegoHostFallsBackToLocalHostWithoutAdvertisedHost(
      String httpAddress, String httpsAddress) throws Exception {
    BaseHttpServer subject = newDisabledServer(httpAddress, httpsAddress);

    assertEquals(InetAddress.getLocalHost().getCanonicalHostName(),
        subject.getSpnegoHost());
  }

  @ParameterizedTest
  @CsvSource({
      "om1.example.com:9874, , om1.example.com",
      "0.0.0.0:9874, om1.example.com:9875, om1.example.com",
      "[2001:db8::1]:9874, , 2001:db8::1",
  })
  void spnegoHostIsAdvertisedHost(String httpAddress, String httpsAddress,
      String expectedHost) throws Exception {
    BaseHttpServer subject = newDisabledServer(httpAddress, httpsAddress);

    assertEquals(expectedHost, subject.getSpnegoHost());
  }

  /**
   * The bind host is what {@link HttpServer2} expands {@code _HOST} to when
   * it is not told otherwise; none of these name the host the keytab was
   * issued for.
   */
  @ParameterizedTest
  @ValueSource(strings = {"0.0.0.0", "::", "127.0.0.1"})
  void spnegoPrincipalNamesAdvertisedHostNotBindHost(String bindHost)
      throws Exception {
    MutableConfigurationSource conf = newConfig(HttpConfig.Policy.HTTP_ONLY);
    conf.set(BIND_HOST_HTTP_KEY, bindHost);
    conf.set(ADDRESS_HTTP_KEY, "om1.example.com:" + BIND_PORT_HTTP_DEFAULT);
    conf.set(HTTP_AUTH_TYPE_KEY, "kerberos");
    conf.set(SPNEGO_PRINCIPAL_KEY, "HTTP/_HOST@EXAMPLE.COM");

    try (BaseHttpServer subject = new SecureTestingHttpServer(conf)) {
      FilterHolder spnegoFilter = httpServer2Of(subject).getWebAppContext()
          .getServletHandler().getFilter(HttpServer2.SPNEGO_FILTER);
      assertEquals("HTTP/om1.example.com@EXAMPLE.COM",
          spnegoFilter.getInitParameter(KerberosAuthenticationHandler.PRINCIPAL));
    }
  }

  private BaseHttpServer newDisabledServer(String httpAddress,
      String httpsAddress) throws IOException {
    MutableConfigurationSource conf = newConfig(HttpConfig.Policy.HTTP_ONLY);
    conf.setBoolean(ENABLED_KEY, false);
    if (httpAddress != null) {
      conf.set(ADDRESS_HTTP_KEY, httpAddress);
    }
    if (httpsAddress != null) {
      conf.set(ADDRESS_HTTPS_KEY, httpsAddress);
    }
    return new TestingHttpServer(conf);
  }

  private static HttpServer2 httpServer2Of(BaseHttpServer server)
      throws ReflectiveOperationException {
    Field field = BaseHttpServer.class.getDeclaredField("httpServer");
    field.setAccessible(true);
    return (HttpServer2) field.get(server);
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
      return KEYTAB_KEY;
    }

    @Override
    protected String getSpnegoPrincipal() {
      return SPNEGO_PRINCIPAL_KEY;
    }

    @Override
    protected String getEnabledKey() {
      return ENABLED_KEY;
    }

    @Override
    protected String getHttpAuthType() {
      return HTTP_AUTH_TYPE_KEY;
    }

    @Override
    protected String getHttpAuthConfigPrefix() {
      return HTTP_AUTH_CONFIG_PREFIX;
    }
  }

  /** Reports HTTP security as enabled without a Kerberos login. */
  private static final class SecureTestingHttpServer extends TestingHttpServer {

    SecureTestingHttpServer(MutableConfigurationSource conf) throws IOException {
      super(conf);
    }

    @Override
    public boolean isSecurityEnabled() {
      return true;
    }
  }

}
