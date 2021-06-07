/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.server.SCMCertStore;
import org.apache.hadoop.hdds.scm.update.client.CRLStore;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Mock CRL Store impl for test.
 */
public class MockCRLStore implements CRLStore {

  private static final String COMPONENT_NAME = "scm";
  private static final Long INITIAL_SEQUENCE_ID = 0L;

  private OzoneConfiguration config;
  private SCMMetadataStore scmMetadataStore;
  private CertificateStore scmCertStore;
  private SecurityConfig securityConfig;
  private KeyPair keyPair;
  private CRLApprover crlApprover;
  private final X509CertificateHolder caCertificateHolder;
  private final Logger log;

  public MockCRLStore(TemporaryFolder tempDir, Logger log) throws Exception {

    this.log = log;
    config = new OzoneConfiguration();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.newFolder().getAbsolutePath());

    securityConfig = new SecurityConfig(config);
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");

    scmMetadataStore = new SCMMetadataStoreImpl(config);
    scmCertStore = new SCMCertStore.Builder().setRatisServer(null)
        .setCRLSequenceId(INITIAL_SEQUENCE_ID)
        .setMetadaStore(scmMetadataStore)
        .build();
    crlApprover = new DefaultCRLApprover(securityConfig,
        keyPair.getPrivate());

    Files.createDirectories(securityConfig.getKeyLocation(COMPONENT_NAME));
    caCertificateHolder = new X509CertificateHolder(generateX509Cert()
        .getEncoded());
  }

  public BigInteger issueCert() throws Exception {
    X509Certificate cert = generateX509Cert();
    scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert,
        HddsProtos.NodeType.SCM);
    return cert.getSerialNumber();
  }

  public Optional<Long> revokeCert(List<BigInteger> certs,
                                   Instant revokeTime) throws IOException {
    log.debug("Revoke certs: ", certs.toString());
    Optional<Long> crlId = scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.lookup(CRLReason.keyCompromise),
        Date.from(revokeTime), crlApprover);
    List<CRLInfo> crlInfos =
        scmCertStore.getCrls(ImmutableList.of(crlId.get()));

    if (crlInfos.isEmpty()) {
      log.debug("CRL[0]: {}", crlInfos.get(0).toString());
    }
    return crlId;
  }


  private X509Certificate generateX509Cert() throws Exception {
    return CertificateCodec.getX509Certificate(
        CertificateCodec.getPEMEncodedString(
            KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
                "SHA256withRSA")));
  }

  @Override
  public long getLatestCrlId() {
    return scmCertStore.getLatestCrlId();
  }

  @Override
  public CRLInfo getCRL(long crlId) throws IOException {
    return scmCertStore.getCrls(Arrays.asList(crlId)).get(0);
  }

  public void close() throws Exception {
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
    }
  }
}
