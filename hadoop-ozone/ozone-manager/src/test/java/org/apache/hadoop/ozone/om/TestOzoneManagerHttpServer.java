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

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test http server of OM with various HTTP option.
 */
@RunWith(value = Parameterized.class)
public class TestOzoneManagerHttpServer {
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestOzoneManagerHttpServer.class.getSimpleName());
  private static String keystoresDir;
  private static String sslConfDir;
  private static OzoneConfiguration conf;
  private static URLConnectionFactory connectionFactory;

  @Parameters public static Collection<Object[]> policy() {
    Object[][] params = new Object[][] {
        {HttpConfig.Policy.HTTP_ONLY},
        {HttpConfig.Policy.HTTPS_ONLY},
        {HttpConfig.Policy.HTTP_AND_HTTPS} };
    return Arrays.asList(params);
  }

  private final HttpConfig.Policy policy;

  public TestOzoneManagerHttpServer(Policy policy) {
    super();
    this.policy = policy;
  }

  @BeforeClass public static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    conf = new OzoneConfiguration();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestOzoneManagerHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterClass public static void tearDown() throws Exception {
    connectionFactory.destroy();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  @Test public void testHttpPolicy() throws Exception {
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY, "localhost");
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY, "localhost");

    OzoneManagerHttpServer server = null;
    try {
      server = new OzoneManagerHttpServer(conf, null);
      DefaultMetricsSystem.initialize("TestOzoneManagerHttpServer");
      server.start();

      Assert.assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      Assert.assertTrue(implies(policy.isHttpEnabled() &&
              !policy.isHttpsEnabled(),
          !canAccess("https", server.getHttpsAddress())));

      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          !canAccess("http", server.getHttpsAddress())));

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
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
