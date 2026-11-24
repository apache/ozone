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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Tests the commonName of the certificates that {@link SCMCertificateClient} requests and creates.
 */
public class TestSCMCertificateClient {
  /** Makes scm-sub@ and scm@ commonNames longer than the RFC 5280 ub-common-name (64), as on Kubernetes. */
  private static final String LONG_HOST_NAME =
      "aaaa-ozone-000-scm-0.aaa-ozone-000-scm-headless.aaaa-data.svc.cluster.local";
  private static final String SHORT_HOST_NAME = "scm1.example.com";

  private SecurityConfig securityConfig;
  private final String scmId = UUID.randomUUID().toString();
  private final String clusterId = UUID.randomUUID().toString();

  @BeforeEach
  public void setUp(@TempDir Path metaDir) {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(HDDS_METADATA_DIR_NAME, metaDir.toString());
    securityConfig = new SecurityConfig(config);
  }

  @Test
  public void testSubCaCsrCommonName() throws Exception {
    new KeyStorage(securityConfig, SCMCertificateClient.COMPONENT_NAME)
        .storeKeyPair(new HDDSKeyGenerator(securityConfig).generateKey());

    assertEquals("scm-sub@" + scmId, subCaCsrCommonName(LONG_HOST_NAME));
    assertEquals("scm-sub@" + SHORT_HOST_NAME, subCaCsrCommonName(SHORT_HOST_NAME));
  }

  @Test
  public void testRootCaCommonName() throws Exception {
    assertEquals("scm@" + scmId, rootCaCommonName(LONG_HOST_NAME, BigInteger.ONE));
    assertEquals("scm@" + SHORT_HOST_NAME, rootCaCommonName(SHORT_HOST_NAME, BigInteger.TEN));
  }

  private String subCaCsrCommonName(String hostName) throws Exception {
    try (SCMCertificateClient client = new SCMCertificateClient(securityConfig, null, scmId, clusterId, null,
        hostName)) {
      PKCS10CertificationRequest csr = client.configureCSRBuilder().build().generateCSR();
      return csr.getSubject().getRDNs(BCStyle.CN)[0].getFirst().getValue().toString();
    }
  }

  private String rootCaCommonName(String hostName, BigInteger rootCertId) throws Exception {
    InetAddress localHost = InetAddress.getByAddress(hostName, new byte[] {127, 0, 0, 1});
    try (SCMCertificateClient client = new SCMCertificateClient(securityConfig, null, scmId, clusterId, null,
        hostName);
        MockedStatic<InetAddress> inetAddress = mockStatic(InetAddress.class, CALLS_REAL_METHODS)) {
      inetAddress.when(InetAddress::getLocalHost).thenReturn(localHost);
      CertificateServer rootCA = client.initializeRootCertificateServer(securityConfig,
          mock(CertificateStore.class), rootCertId, new DefaultCAProfile(),
          SCM_ROOT_CA_COMPONENT_NAME + "-" + rootCertId);
      X509Certificate caCert = rootCA.getCACertificate();
      return X500Name.getInstance(caCert.getSubjectX500Principal().getEncoded())
          .getRDNs(BCStyle.CN)[0].getFirst().getValue().toString();
    }
  }
}
