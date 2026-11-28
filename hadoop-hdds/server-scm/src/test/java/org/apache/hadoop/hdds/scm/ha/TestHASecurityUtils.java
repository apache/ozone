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

package org.apache.hadoop.hdds.scm.ha;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.LONG_HOST_NAME;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.getCommonName;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CommonNameTestUtil.withLocalHostName;
import static org.apache.hadoop.ozone.OzoneConsts.SCM_ROOT_CA_COMPONENT_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultCAProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link HASecurityUtils}.
 */
public class TestHASecurityUtils {
  private final String scmId = UUID.randomUUID().toString();
  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;

  @BeforeEach
  public void setUp(@TempDir Path metaDir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, metaDir.toString());
    scmStorageConfig = new SCMStorageConfig(conf);
    scmStorageConfig.setScmId(scmId);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
  }

  /**
   * The root CA certificate, also the new one of a root CA rotation, is scm@host name, or scm@SCM ID when that is
   * longer than the RFC 5280 ub-common-name (64), as on Kubernetes.
   */
  @Test
  public void testRootCaCommonName() throws Exception {
    assertEquals("scm@" + scmId, rootCaCommonName(LONG_HOST_NAME, BigInteger.ONE));
    assertEquals("scm@scm1.example.com", rootCaCommonName("scm1.example.com", BigInteger.TEN));
  }

  private String rootCaCommonName(String hostName, BigInteger rootCertId) throws Exception {
    return withLocalHostName(hostName, () -> getCommonName(HASecurityUtils.initializeRootCertificateServer(
        new SecurityConfig(conf), mock(CertificateStore.class), scmStorageConfig, rootCertId, new DefaultCAProfile(),
        SCM_ROOT_CA_COMPONENT_NAME + "-" + rootCertId).getCACertificate()));
  }
}
