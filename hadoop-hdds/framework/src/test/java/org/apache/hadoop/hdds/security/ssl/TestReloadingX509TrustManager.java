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
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.X509Certificate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Test ReloadingX509TrustManager.
 */
public class TestReloadingX509TrustManager {
  private final LogCapturer reloaderLog =
      LogCapturer.captureLogs(ReloadingX509TrustManager.LOG);
  private static CertificateClientTestImpl caClient;

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    caClient = new CertificateClientTestImpl(conf);
  }

  @Test
  public void testReload() throws Exception {
    ReloadingX509TrustManager tm =
        (ReloadingX509TrustManager) caClient.getServerKeyStoresFactory()
            .getTrustManagers()[0];
    X509Certificate cert1 = caClient.getRootCACertificate();
    assertThat(tm.getAcceptedIssuers(),
        Matchers.arrayContaining(cert1));

    caClient.renewRootCA();
    caClient.renewKey();
    X509Certificate cert2 = caClient.getRootCACertificate();
    assertNotEquals(cert1, cert2);

    assertThat(tm.getAcceptedIssuers(), hasItemInArray(cert1));
    assertThat(tm.getAcceptedIssuers(), hasItemInArray(cert2));
    assertThat(reloaderLog.getOutput(),
        containsString("ReloadingX509TrustManager is reloaded"));

    // Make sure there are two reload happened, one for server, one for client
    assertEquals(2, StringUtils.countMatches(reloaderLog.getOutput(),
        "ReloadingX509TrustManager is reloaded"));
  }
}
