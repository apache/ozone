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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.PrivateKey;
import java.util.Timer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test ReloadingX509KeyManager.
 */
public class TestReloadingX509KeyManager {
  private final LogCapturer reloaderLog =
      LogCapturer.captureLogs(ReloadingX509KeyManager.LOG);
  private static OzoneConfiguration conf;
  private static CertificateClientTestImpl caClient;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    caClient = new CertificateClientTestImpl(conf);
  }

  @Test
  public void testReload() throws Exception {
    int reloadInterval = 1000;
    Timer fileMonitoringTimer = new Timer(true);
    ReloadingX509KeyManager km = new ReloadingX509KeyManager("jks", caClient);
    try {
      fileMonitoringTimer.schedule(new MonitoringTimerTask(caClient,
          km::loadFrom, null), reloadInterval, reloadInterval);
      PrivateKey privateKey1 = caClient.getPrivateKey();
      assertEquals(privateKey1,
          km.getPrivateKey(caClient.getComponentName() + "_key"));

      caClient.renewRootCA();
      caClient.renewKey();
      PrivateKey privateKey2 = caClient.getPrivateKey();
      assertNotEquals(privateKey1, privateKey2);

      // Wait so that the new certificate get reloaded
      Thread.sleep((reloadInterval + 1000));
      assertEquals(privateKey2,
          km.getPrivateKey(caClient.getComponentName() + "_key"));

      assertTrue(reloaderLog.getOutput().contains(
          "ReloadingX509KeyManager is reloaded"));

      // Make sure there is only one reload happened.
      GenericTestUtils.waitFor(
          () -> 1 == StringUtils.countMatches(reloaderLog.getOutput(),
              "ReloadingX509KeyManager is reloaded"),
          1000, reloadInterval + 1000);
    } finally {
      fileMonitoringTimer.cancel();
    }
  }
}
