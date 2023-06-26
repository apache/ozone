/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

/**
 * Test class for {@link SCMSecurityProtocolServer}.
 */
@Timeout(20)
public class TestSCMSecurityProtocolServer {
  private SCMSecurityProtocolServer securityProtocolServer;
  private OzoneConfiguration config;
  @Mock
  private StorageContainerManager mockScm;
  @Mock
  private SCMStorageConfig storageConfigMock;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    config = new OzoneConfiguration();
    config.set(OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
        OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT + ":0");
    securityProtocolServer = new SCMSecurityProtocolServer(config, null,
        null, new ArrayList<>(), mockScm, null);
  }

  @AfterEach
  public void tearDown() {
    if (securityProtocolServer != null) {
      securityProtocolServer.stop();
      securityProtocolServer = null;
    }
    config = null;
  }

  @Test
  public void testStart() throws IOException {
    securityProtocolServer.start();
  }

  @Test
  public void testStop() {
    securityProtocolServer.stop();
  }

  @Test
  public void testReturnLastRootCa() throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate oldRootCa = KeyStoreTestUtil.generateCertificate("CN=dn",
        keyPair, 15, "SHA256withRSA");
    X509Certificate latestRootCa = KeyStoreTestUtil.generateCertificate("CN=dn",
        keyPair, 30, "SHA256withRSA");
    Assertions.assertTrue(oldRootCa.getNotAfter().toInstant()
        .isBefore(latestRootCa.getNotAfter().toInstant()));
    securityProtocolServer.addNewRootCa(oldRootCa);
    securityProtocolServer.addNewRootCa(latestRootCa);
    String pemEncodedLatestRootCa =
        CertificateCodec.getPEMEncodedString(latestRootCa);
    Mockito.when(mockScm.getScmStorageConfig()).thenReturn(storageConfigMock);
    Mockito.when(
        storageConfigMock.checkPrimarySCMIdInitialized()).thenReturn(true);
    Assertions.assertEquals(securityProtocolServer.getRootCACertificate(),
        pemEncodedLatestRootCa);
  }
}
