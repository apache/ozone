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
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.LONG_HOST_NAME;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.getCommonName;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.getCsrCommonName;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.withLocalHostName;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the commonName of the certificates that {@link SCMCertificateClient} requests and creates.
 */
public class TestSCMCertificateClient {
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
    try (SCMCertificateClient client = newClient(hostName)) {
      return getCsrCommonName(client);
    }
  }

  private String rootCaCommonName(String hostName, BigInteger rootCertId) throws Exception {
    try (SCMCertificateClient client = newClient(hostName)) {
      return withLocalHostName(hostName, () -> getCommonName(client.initializeRootCertificateServer(securityConfig,
          mock(CertificateStore.class), rootCertId, new DefaultCAProfile(),
          SCM_ROOT_CA_COMPONENT_NAME + "-" + rootCertId).getCACertificate()));
    }
  }

  private SCMCertificateClient newClient(String hostName) {
    return new SCMCertificateClient(securityConfig, null, scmId, clusterId, null, hostName);
  }
}
