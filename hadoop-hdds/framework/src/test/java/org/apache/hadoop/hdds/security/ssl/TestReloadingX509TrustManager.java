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

package org.apache.hadoop.hdds.security.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.security.cert.X509Certificate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test ReloadingX509TrustManager.
 */
public class TestReloadingX509TrustManager {
  private final LogCapturer reloaderLog =
      LogCapturer.captureLogs(ReloadingX509TrustManager.class);
  private static CertificateClientTestImpl caClient;

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    caClient = new CertificateClientTestImpl(conf);
  }

  @Test
  public void testReload() throws Exception {
    ReloadingX509TrustManager tm = caClient.getTrustManager();
    X509Certificate cert1 = caClient.getRootCACertificate();
    assertThat(tm.getAcceptedIssuers()).containsOnly(cert1);

    caClient.renewRootCA();
    caClient.renewKey();
    X509Certificate cert2 = caClient.getRootCACertificate();
    assertNotEquals(cert1, cert2);

    assertThat(tm.getAcceptedIssuers()).contains(cert1, cert2);
    assertThat(reloaderLog.getOutput())
        .contains("ReloadingX509TrustManager is reloaded");

    // Only one reload has to happen for the CertificateClient's trustManager.
    assertEquals(1, StringUtils.countMatches(reloaderLog.getOutput(), "ReloadingX509TrustManager is reloaded"));
  }
}
