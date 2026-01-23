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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManagerHttpServer;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test http server os SCM with various HTTP option.
 */
public class TestStorageContainerManagerHttpServer {
  @TempDir
  private static File baseDir;
  private static String keystoresDir;
  private static String sslConfDir;
  private static OzoneConfiguration conf;
  private static URLConnectionFactory connectionFactory;

  @BeforeAll
  public static void setUp() throws Exception {
    File ozoneMetadataDirectory = new File(baseDir, "metadata");
    assertTrue(ozoneMetadataDirectory.mkdirs());
    conf = new OzoneConfiguration();
    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestStorageContainerManagerHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        ozoneMetadataDirectory.getAbsolutePath());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    connectionFactory.destroy();
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @ParameterizedTest
  @EnumSource(HttpConfig.Policy.class)
  public void testHttpPolicy(HttpConfig.Policy policy) throws Exception {
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_KEY, "localhost");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTPS_BIND_HOST_KEY, "localhost");

    StorageContainerManagerHttpServer server = null;
    try {
      DefaultMetricsSystem.initialize("TestStorageContainerManagerHttpServer");
      server = new StorageContainerManagerHttpServer(conf, null);
      server.start();

      assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      assertTrue(implies(policy.isHttpEnabled() &&
              !policy.isHttpsEnabled(),
          !canAccess("https", server.getHttpsAddress())));

      assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      assertTrue(implies(policy.isHttpsEnabled() &&
              !policy.isHttpEnabled(),
          !canAccess("http", server.getHttpAddress())));

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
      URL url =
          new URL(scheme + "://" + NetUtils.getHostPortString(addr) + "/jmx");
      URLConnection conn = connectionFactory.openConnection(url);
      conn.connect();
      conn.getContent();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
