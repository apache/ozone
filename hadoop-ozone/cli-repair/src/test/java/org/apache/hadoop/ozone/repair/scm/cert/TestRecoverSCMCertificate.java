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

package org.apache.hadoop.ozone.repair.scm.cert;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_SUB_CA_PREFIX;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests how {@link RecoverSCMCertificate} finds the certificates of this SCM: by host name, or by SCM ID for a
 * commonName that would be longer than 64 characters with the host name.
 */
public class TestRecoverSCMCertificate {
  /** Longer than 64 characters with the scm-sub@ or scm@ prefix, so its certificates are named by SCM ID. */
  private static final String LONG_HOST_NAME =
      "aaaa-ozone-000-scm-0.aaa-ozone-000-scm-headless.aaaa-data.svc.cluster.local";
  private static final String SHORT_HOST_NAME = "scm1.example.com";

  private final String scmId = UUID.randomUUID().toString();
  private SecurityConfig securityConfig;

  @BeforeEach
  public void setUp(@TempDir Path metaDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, metaDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testGetSubCertificate() throws Exception {
    X509Certificate byHostName = certificate(SCM_SUB_CA_PREFIX + SHORT_HOST_NAME, 1);
    X509Certificate byScmId = certificate(SCM_SUB_CA_PREFIX + scmId, 2);
    X509Certificate otherScm = certificate(SCM_SUB_CA_PREFIX + UUID.randomUUID(), 3);
    Map<BigInteger, X509Certificate> allCerts = new HashMap<>();
    for (X509Certificate cert : new X509Certificate[] {byHostName, byScmId, otherScm}) {
      allCerts.put(cert.getSerialNumber(), cert);
    }

    assertSame(byHostName, RecoverSCMCertificate.getSubCertificate(allCerts, SHORT_HOST_NAME, scmId));
    assertSame(byScmId, RecoverSCMCertificate.getSubCertificate(allCerts, LONG_HOST_NAME, scmId));
    // Without the SCM ID, as when the SCM storage cannot be read, only the host name matches.
    assertSame(byHostName, RecoverSCMCertificate.getSubCertificate(allCerts, SHORT_HOST_NAME, null));
    assertThrows(Exception.class, () -> RecoverSCMCertificate.getSubCertificate(allCerts, LONG_HOST_NAME, null));
  }

  @Test
  public void testIsRootCA() throws Exception {
    X509Certificate byHostName = certificate(SCM_ROOT_CA_PREFIX + SHORT_HOST_NAME, 1);
    X509Certificate byScmId = certificate(SCM_ROOT_CA_PREFIX + scmId, 2);

    assertTrue(RecoverSCMCertificate.isRootCA(byHostName, SHORT_HOST_NAME, scmId));
    assertTrue(RecoverSCMCertificate.isRootCA(byHostName, SHORT_HOST_NAME, null));
    assertTrue(RecoverSCMCertificate.isRootCA(byScmId, LONG_HOST_NAME, scmId));
    assertFalse(RecoverSCMCertificate.isRootCA(byScmId, LONG_HOST_NAME, null));
    assertFalse(RecoverSCMCertificate.isRootCA(byScmId, LONG_HOST_NAME, UUID.randomUUID().toString()));
  }

  private X509Certificate certificate(String subject, long serial) throws Exception {
    ZonedDateTime now = ZonedDateTime.now();
    return SelfSignedCertificate.newBuilder()
        .setBeginDate(now)
        .setEndDate(now.plusDays(1))
        .setClusterID(UUID.randomUUID().toString())
        .setScmID(scmId)
        .setSubject(subject)
        .setKey(new HDDSKeyGenerator(securityConfig).generateKey())
        .setConfiguration(securityConfig)
        .makeCA(BigInteger.valueOf(serial))
        .build();
  }
}
