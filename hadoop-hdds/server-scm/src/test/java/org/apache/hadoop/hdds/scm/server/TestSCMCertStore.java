/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.apache.hadoop.ozone.OzoneConsts.CRL_SEQUENCE_ID_KEY;

/**
 * Test class for @{@link SCMCertStore}.
 */
public class TestSCMCertStore {

  private static final String COMPONENT_NAME = "scm";
  private static final Long INITIAL_SEQUENCE_ID = 1L;

  private OzoneConfiguration config;
  private SCMMetadataStore scmMetadataStore;
  private SCMCertStore scmCertStore;
  private SecurityConfig securityConfig;
  private X509Certificate x509Certificate;
  private KeyPair keyPair;
  private CRLApprover crlApprover;

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();

    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.newFolder().getAbsolutePath());

    securityConfig = new SecurityConfig(config);
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
  }

  @Before
  public void initDbStore() throws IOException {
    scmMetadataStore = new SCMMetadataStoreImpl(config);
    scmCertStore = new SCMCertStore(scmMetadataStore, INITIAL_SEQUENCE_ID);
  }

  @Before
  public void generateCertificate() throws Exception {
    Files.createDirectories(securityConfig.getKeyLocation(COMPONENT_NAME));
    x509Certificate = generateX509Cert();
  }

  @Before
  public void initCRLApprover() {
    crlApprover = new DefaultCRLApprover(securityConfig,
        keyPair.getPrivate());
  }

  @After
  public void destroyDbStore() throws Exception {
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
    }
  }

  @Test
  public void testRevokeCertificates() throws Exception {

    BigInteger serialID = x509Certificate.getSerialNumber();
    scmCertStore.storeValidCertificate(serialID, x509Certificate);
    Date now = new Date();

    assertNotNull(
        scmCertStore.getCertificateByID(serialID,
        CertificateStore.CertType.VALID_CERTS));

    X509CertificateHolder caCertificateHolder =
        new X509CertificateHolder(generateX509Cert().getEncoded());
    List<BigInteger> certs = new ArrayList<>();
    certs.add(x509Certificate.getSerialNumber());
    Optional<Long> sequenceId = scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.lookup(CRLReason.keyCompromise), now, crlApprover);

    assertTrue(sequenceId.isPresent());
    assertEquals(INITIAL_SEQUENCE_ID + 1L, (long) sequenceId.get());

    assertNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.VALID_CERTS));

    assertNotNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.REVOKED_CERTS));

    // CRL Info table should have a CRL with sequence id
    assertNotNull(scmMetadataStore.getCRLInfoTable()
        .get(sequenceId.get()));

    // Check the sequence ID table for latest sequence id
    assertEquals(INITIAL_SEQUENCE_ID + 1L, (long)
        scmMetadataStore.getCRLSequenceIdTable().get(CRL_SEQUENCE_ID_KEY));

    CRLInfo crlInfo =
        scmMetadataStore.getCRLInfoTable().get(sequenceId.get());

    Set<? extends X509CRLEntry> revokedCertificates =
        crlInfo.getX509CRL().getRevokedCertificates();
    assertEquals(1L, revokedCertificates.size());
    assertEquals(x509Certificate.getSerialNumber(),
        revokedCertificates.iterator().next().getSerialNumber());

    // Now trying to revoke the already revoked certificate should result in
    // a warning message and no-op. It should not create a new CRL.
    sequenceId = scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.lookup(CRLReason.unspecified), now, crlApprover);

    assertFalse(sequenceId.isPresent());

    assertEquals(1L,
        getTableSize(scmMetadataStore.getCRLInfoTable().iterator()));

    // Generate 3 more certificates and revoke 2 of them
    List<BigInteger> newSerialIDs = new ArrayList<>();
    for (int i = 0; i<3; i++) {
      X509Certificate cert = generateX509Cert();
      scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert);
      newSerialIDs.add(cert.getSerialNumber());
    }

    // Add the first 2 certificates to the revocation list
    sequenceId = scmCertStore.revokeCertificates(newSerialIDs.subList(0, 2),
        caCertificateHolder,
        CRLReason.lookup(CRLReason.aACompromise), now, crlApprover);

    // This should create a CRL with sequence id INITIAL_SEQUENCE_ID + 2
    // And contain 2 certificates in it
    assertTrue(sequenceId.isPresent());
    assertEquals(INITIAL_SEQUENCE_ID + 2L, (long) sequenceId.get());

    // Check the sequence ID table for latest sequence id
    assertEquals(INITIAL_SEQUENCE_ID + 2L, (long)
        scmMetadataStore.getCRLSequenceIdTable().get(CRL_SEQUENCE_ID_KEY));

    CRLInfo newCrlInfo = scmMetadataStore.getCRLInfoTable()
        .get(sequenceId.get());
    revokedCertificates = newCrlInfo.getX509CRL().getRevokedCertificates();
    assertEquals(2L, revokedCertificates.size());
    assertNotNull(
        revokedCertificates.stream().filter(c ->
            c.getSerialNumber().equals(newSerialIDs.get(0)))
            .findAny());

    assertNotNull(
        revokedCertificates.stream().filter(c ->
            c.getSerialNumber().equals(newSerialIDs.get(1)))
            .findAny());

    // Valid certs table should have 1 cert
    assertEquals(1L,
        getTableSize(scmMetadataStore.getValidCertsTable().iterator()));
    // Make sure that the last certificate that was not revoked is the one
    // in the valid certs table.
    assertEquals(newSerialIDs.get(2),
        scmMetadataStore.getValidCertsTable().iterator().next().getKey());

    // Revoked certs table should have 3 certs
    assertEquals(3L,
        getTableSize(scmMetadataStore.getRevokedCertsTable().iterator()));
  }

  @Test
  public void testRevokeCertificatesForFutureTime() throws Exception {
    BigInteger serialID = x509Certificate.getSerialNumber();
    scmCertStore.storeValidCertificate(serialID, x509Certificate);
    Date now = new Date();
    // Set revocation time in the future
    Date revocationTime = new Date(now.getTime()+500);


    X509CertificateHolder caCertificateHolder =
        new X509CertificateHolder(generateX509Cert().getEncoded());
    List<BigInteger> certs = new ArrayList<>();
    certs.add(x509Certificate.getSerialNumber());
    Optional<Long> sequenceId = scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.lookup(CRLReason.keyCompromise), revocationTime,
        crlApprover);

    assertTrue(sequenceId.isPresent());
    assertEquals(INITIAL_SEQUENCE_ID + 1L, (long) sequenceId.get());

    assertNotNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.VALID_CERTS));

    assertNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.REVOKED_CERTS));
  }

  private X509Certificate generateX509Cert() throws Exception {
    return CertificateCodec.getX509Certificate(
        CertificateCodec.getPEMEncodedString(
            KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        "SHA256withRSA")));
  }

  private long getTableSize(Iterator iterator) {
    long size = 0;

    while(iterator.hasNext()) {
      size++;
      iterator.next();
    }

    return size;
  }
}
