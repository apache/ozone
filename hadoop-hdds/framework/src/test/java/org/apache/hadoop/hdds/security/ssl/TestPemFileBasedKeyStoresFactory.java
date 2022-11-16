/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.ssl;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.CertificateClientTest;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test PemFileBasedKeyStoresFactory.
 */
public class TestPemFileBasedKeyStoresFactory {
  private static OzoneConfiguration conf;
  private static CertificateClientTest caClient;

  @Test
  public void testInit() throws Exception {
    clientMode(true);
    clientMode(false);
    serverMode(true);
    serverMode(false);
  }

  private void clientMode(boolean clientAuth) throws Exception {
    conf = new OzoneConfiguration();
    caClient = new CertificateClientTest(conf);
    SecurityConfig secConf = new SecurityConfig(conf);
    KeyStoresFactory keyStoresFactory = new PemFileBasedKeyStoresFactory(
        secConf, caClient);

    try {
      keyStoresFactory.init(SSLFactory.Mode.CLIENT, clientAuth);
      if (clientAuth) {
        Assert.assertTrue(keyStoresFactory.getKeyManagers()[0]
            instanceof ReloadingX509KeyManager);
      } else {
        Assert.assertFalse(keyStoresFactory.getKeyManagers()[0]
            instanceof ReloadingX509KeyManager);
      }
      Assert.assertTrue(keyStoresFactory.getTrustManagers()[0]
          instanceof ReloadingX509TrustManager);
    } finally {
      keyStoresFactory.destroy();
    }
  }

  private void serverMode(boolean clientAuth) throws Exception {
    conf = new OzoneConfiguration();
    caClient = new CertificateClientTest(conf);
    SecurityConfig secConf = new SecurityConfig(conf);
    KeyStoresFactory keyStoresFactory = new PemFileBasedKeyStoresFactory(
        secConf, caClient);
    try {
      keyStoresFactory.init(SSLFactory.Mode.SERVER, clientAuth);
      Assert.assertTrue(keyStoresFactory.getKeyManagers()[0]
          instanceof ReloadingX509KeyManager);
      Assert.assertTrue(keyStoresFactory.getTrustManagers()[0]
          instanceof ReloadingX509TrustManager);
    } finally {
      keyStoresFactory.destroy();
    }
  }
}
