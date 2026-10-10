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

package org.apache.hadoop.ozone.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.s3secret.S3SecretConfigKeys;
import org.apache.hadoop.ozone.s3sts.S3STSConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.ozone.test.GenericTestUtils.PortAllocator;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.webapp.WebAppContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the CDI wiring of the S3 Gateway HTTP servers.
 */
class TestS3GatewayHttpServers {

  /**
   * Attribute Weld's JettyContainer probes for, set by CdiDecoratingListener.
   */
  private static final String CDI_ATTRIBUTE = "org.eclipse.jetty.cdi";
  private static final String CDI_MODE = "CdiDecoratingListener";

  @TempDir
  private Path tempDir;

  /**
   * Switching {@link UserGroupInformation} to Kerberos reads the default
   * realm, which the JDK resolves from these properties when there is no
   * krb5.conf, as in CI.
   */
  @BeforeAll
  static void fakeKerberosRealm() {
    System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
    System.setProperty("java.security.krb5.kdc", "localhost");
  }

  @AfterAll
  static void clearKerberosRealm() {
    System.clearProperty("java.security.krb5.realm");
    System.clearProperty("java.security.krb5.kdc");
  }

  /**
   * An enabled server gets Jetty's CDI integration attribute, without which
   * Weld falls through to its legacy Jetty container and fails to start the
   * context on Jetty 12.
   */
  @Test
  void enabledServerInstallsCdiIntegrationAttribute() throws Exception {
    OzoneConfiguration conf = newConf();
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY,
        PortAllocator.localhostWithFreePort());

    ExposedS3GatewayHttpServer server = new ExposedS3GatewayHttpServer(conf);

    assertEquals(CDI_MODE, server.context().getAttribute(CDI_ATTRIBUTE));
  }

  /**
   * A disabled server never builds its HTTP server, so it has no web app
   * context: installing the CDI listener must be skipped rather than fail. The
   * STS server is disabled by default, so the S3 Gateway would not start at all.
   */
  @Test
  void disabledStsServerIsCreatedWithoutCdiIntegration() {
    OzoneConfiguration conf = newConf();
    assertFalse(conf.getBoolean(S3STSConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY,
        OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_DEFAULT),
        "the STS server is expected to be disabled by default");

    assertDoesNotThrow(() -> new S3STSHttpServer(conf, "s3g-sts"));
  }

  /**
   * The secret endpoint expands {@code _HOST} in its SPNEGO principal from the
   * advertised web admin address, not from the bind host, which here is the
   * IPv6 wildcard that no keytab entry can match.
   */
  @Test
  void secretEndpointPrincipalNamesAdvertisedHost() throws Exception {
    OzoneConfiguration conf = newConf();
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_BIND_HOST_KEY, "::");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY,
        "s3g1.example.com:" + PortAllocator.getFreePort());
    conf.setBoolean(S3SecretConfigKeys.OZONE_S3G_SECRET_HTTP_ENABLED_KEY, true);
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL,
        "HTTP/_HOST@EXAMPLE.COM");

    Configuration hadoopConf = new Configuration();
    hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(hadoopConf);
    try (ExposedS3GatewayWebAdminServer server =
        new ExposedS3GatewayWebAdminServer(conf)) {
      FilterHolder secretFilter = server.context().getServletHandler()
          .getFilter("secretAuthentication");
      assertEquals("HTTP/s3g1.example.com@EXAMPLE.COM",
          secretFilter.getInitParameter(KerberosAuthenticationHandler.PRINCIPAL));
    } finally {
      UserGroupInformation.reset();
    }
  }

  private OzoneConfiguration newConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_BASEDIR,
        tempDir.resolve("http").toString());
    return conf;
  }

  /** Exposes the web app context, which BaseHttpServer keeps protected. */
  private static final class ExposedS3GatewayHttpServer
      extends S3GatewayHttpServer {

    ExposedS3GatewayHttpServer(MutableConfigurationSource conf)
        throws IOException {
      super(conf, "s3gateway");
    }

    WebAppContext context() {
      return getWebAppContext();
    }
  }

  /** Exposes the web app context, which BaseHttpServer keeps protected. */
  private static final class ExposedS3GatewayWebAdminServer
      extends S3GatewayWebAdminServer {

    ExposedS3GatewayWebAdminServer(MutableConfigurationSource conf)
        throws IOException {
      super(conf, "s3g-web");
    }

    WebAppContext context() {
      return getWebAppContext();
    }
  }
}
