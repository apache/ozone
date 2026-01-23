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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.DATANODE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.OM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.returnsLastArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for @{@link SCMCertStore}.
 */
public class TestSCMCertStore {

  private static final String COMPONENT_NAME = "scm";

  private SCMMetadataStore scmMetadataStore;
  private CertificateStore scmCertStore;
  private KeyPair keyPair;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();

    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.toAbsolutePath().toString());

    SecurityConfig securityConfig = new SecurityConfig(config);
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");

    final SCMRatisServer ratisServer = mock(SCMRatisServer.class);
    when(ratisServer.getProxyHandler(eq(SCMRatisProtocol.RequestType.CERT_STORE),
        eq(CertificateStore.class), any(CertificateStore.class)))
        .then(returnsLastArg());
    scmMetadataStore = new SCMMetadataStoreImpl(config);
    scmCertStore = new SCMCertStore.Builder().setRatisServer(ratisServer)
        .setMetadaStore(scmMetadataStore)
        .build();

    Files.createDirectories(securityConfig.getKeyLocation(COMPONENT_NAME));
  }

  @AfterEach
  public void destroyDbStore() throws Exception {
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
    }
  }

  private X509Certificate generateX509Cert() throws Exception {
    return KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
            "SHA256withRSA");
  }

  @Test
  public void testGetAndListCertificates() throws Exception {
    X509Certificate cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert, SCM);
    checkListCerts(SCM, 1);

    cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert, SCM);
    checkListCerts(SCM, 2);

    cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert, SCM);
    checkListCerts(SCM, 3);

    cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert, OM);

    // As for OM and DN all certs in valid certs table are returned.
    // This test can be fixed once we have code for returning OM/DN certs.
    checkListCerts(OM, 4);

    cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert, DATANODE);
    checkListCerts(OM, 5);

  }

  @Test
  public void testRemoveAllCertificates() throws Exception {
    X509Certificate scmCert = CertificateTestUtils.createSelfSignedCert(
        keyPair, "1", Duration.ofDays(1), BigInteger.valueOf(1));
    X509Certificate expiredScmCert = CertificateTestUtils.createSelfSignedCert(
        keyPair, "2", Duration.ofNanos(1), BigInteger.valueOf(2));
    X509Certificate nonScmCert = CertificateTestUtils.createSelfSignedCert(
        keyPair, "3", Duration.ofDays(1), BigInteger.valueOf(3));
    X509Certificate expiredNonScmCert =
        CertificateTestUtils.createSelfSignedCert(
            keyPair, "4", Duration.ofNanos(1), BigInteger.valueOf(4));
    scmCertStore.storeValidCertificate(
        scmCert.getSerialNumber(), scmCert, SCM);
    scmCertStore.storeValidCertificate(
        expiredScmCert.getSerialNumber(), expiredScmCert, SCM);
    scmCertStore.storeValidCertificate(
        nonScmCert.getSerialNumber(), nonScmCert, OM);
    scmCertStore.storeValidCertificate(
        expiredNonScmCert.getSerialNumber(), expiredNonScmCert, OM);
    //Listing OM certs still lists SCM certificates as well
    checkListCerts(OM, 4);
    checkListCerts(SCM, 2);
    scmCertStore.removeAllExpiredCertificates();
    checkListCerts(OM, 2);
    checkListCerts(SCM, 1);
  }

  private void checkListCerts(NodeType role, int expected) throws Exception {
    List<X509Certificate> certificateList = scmCertStore.listCertificate(role,
        BigInteger.valueOf(0), 10);
    assertEquals(expected, certificateList.size());
  }
}
