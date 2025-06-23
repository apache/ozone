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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test http server of OM with various HTTP option.
 */
public class TestOzoneManagerHttpServer {
  private static String keystoresDir;
  private static String sslConfDir;
  private static OzoneConfiguration conf;
  private static URLConnectionFactory connectionFactory;
  private static File ozoneMetadataDirectory;

  public static Collection<Object[]> policy() {
    Object[][] params = new Object[][] {
        {HttpConfig.Policy.HTTP_ONLY},
        {HttpConfig.Policy.HTTPS_ONLY},
        {HttpConfig.Policy.HTTP_AND_HTTPS} };
    return Arrays.asList(params);
  }

  @BeforeAll public static void setUp(@TempDir File baseDir) throws Exception {
    // Create metadata directory
    ozoneMetadataDirectory = new File(baseDir.getPath(), "metadata");
    assertTrue(ozoneMetadataDirectory.mkdirs());

    // Initialize the OzoneConfiguration
    conf = new OzoneConfiguration();
    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestOzoneManagerHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    // Set up OM HTTP and HTTPS addresses
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        ozoneMetadataDirectory.getAbsolutePath());
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY, "localhost");
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY, "localhost");
  }

  @AfterAll public static void tearDown() throws Exception {
    connectionFactory.destroy();
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @ParameterizedTest
  @MethodSource("policy")
  public void testHttpPolicy(HttpConfig.Policy policy) throws Exception {
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    OzoneManagerHttpServer server = null;
    try {
      server = new OzoneManagerHttpServer(conf, null);
      DefaultMetricsSystem.initialize("TestOzoneManagerHttpServer");
      server.start();

      assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      assertTrue(implies(policy.isHttpEnabled() &&
              !policy.isHttpsEnabled(),
          !canAccess("https", server.getHttpsAddress())));

      assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      assertTrue(implies(policy.isHttpsEnabled(),
          !canAccess("http", server.getHttpsAddress())));
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test
  // Verify if jetty-dir will be created inside ozoneMetadataDirectory path
  public void testJettyDirectoryCreation() throws Exception {
    OzoneManagerHttpServer server = null;
    try {
      server = new OzoneManagerHttpServer(conf, null);
      DefaultMetricsSystem.initialize("TestOzoneManagerHttpServer");
      server.start();
      // Checking if the /webserver directory does get created
      File webServerDir =
          new File(ozoneMetadataDirectory, BaseHttpServer.SERVER_DIR);
      assertTrue(webServerDir.exists());
      // Verify that the jetty directory is set correctly
      String expectedJettyDirLocation =
          ozoneMetadataDirectory.getAbsolutePath() + BaseHttpServer.SERVER_DIR;
      assertEquals(expectedJettyDirLocation, server.getJettyBaseTmpDir());
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private static boolean canAccess(String scheme, InetSocketAddress addr) {
    if (addr == null) {
      return false;
    }
    try {
      URL url = new URL(scheme + "://" + NetUtils.getHostPortString(addr) + "/jmx");
      URLConnection conn = connectionFactory.openConnection(url);
      conn.connect();
      conn.getContent();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
