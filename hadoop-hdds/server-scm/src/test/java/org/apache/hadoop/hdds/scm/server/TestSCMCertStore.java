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
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.junit.After;
import org.junit.Assert;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();

    config.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempDir.newFolder().getAbsolutePath());

    securityConfig = new SecurityConfig(config);
  }

  @Before
  public void initDbStore() throws IOException {
    scmMetadataStore = new SCMMetadataStoreImpl(config);
    scmCertStore = new SCMCertStore(scmMetadataStore, INITIAL_SEQUENCE_ID);
  }

  @Before
  public void generateCertificate() throws Exception {
    Files.createDirectories(securityConfig.getKeyLocation(COMPONENT_NAME));
    x509Certificate = generateX509Cert(null);
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

    Assert.assertNotNull(
        scmCertStore.getCertificateByID(serialID,
        CertificateStore.CertType.VALID_CERTS));

    X509CertificateHolder caCertificateHolder =
        new X509CertificateHolder(generateX509Cert(keyPair).getEncoded());
    List<X509Certificate> certs = new ArrayList<>();
    certs.add(x509Certificate);
    scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.unspecified, securityConfig,
        keyPair);

    Assert.assertNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.VALID_CERTS));

    Assert.assertNotNull(
        scmCertStore.getCertificateByID(serialID,
            CertificateStore.CertType.REVOKED_CERTS));

    // CRL Info table should have a CRL with sequence id
    Assert.assertEquals(
        INITIAL_SEQUENCE_ID + 1L,
        (long) scmMetadataStore.getCRLInfoTable().iterator().next().getKey());

    // Check the sequence ID table for latest sequence id
    Assert.assertEquals(INITIAL_SEQUENCE_ID + 1L, (long)
        scmMetadataStore.getCRLSequenceIdTable().get(CRL_SEQUENCE_ID_KEY));

    CRLInfo crlInfo =
        scmMetadataStore.getCRLInfoTable().iterator().next().getValue();

    Set<? extends X509CRLEntry> revokedCertificates =
        crlInfo.getX509CRL().getRevokedCertificates();
    Assert.assertEquals(1L, revokedCertificates.size());
    Assert.assertEquals(x509Certificate.getSerialNumber(),
        revokedCertificates.iterator().next().getSerialNumber());

    // Now trying to revoke the already revoked certificate should result in
    // a warning message and no-op. It should not create a new CRL.
    scmCertStore.revokeCertificates(certs,
        caCertificateHolder,
        CRLReason.unspecified, securityConfig,
        keyPair);

    int size = 0;
    TableIterator<Long, ? extends Table.KeyValue<Long, CRLInfo>> iter =
        scmMetadataStore.getCRLInfoTable().iterator();

    while(iter.hasNext()) {
      size++;
      iter.next();
    }

    Assert.assertEquals(1, size);

    // Generate 3 more certificates and revoke 2 of them
    List<X509Certificate> newCerts = new ArrayList<>();
    for (int i = 0; i<3; i++) {
      X509Certificate cert = generateX509Cert(keyPair);
      scmCertStore.storeValidCertificate(cert.getSerialNumber(), cert);
      newCerts.add(cert);
    }

    // Add the first 2 certificates to the revocation list
    scmCertStore.revokeCertificates(newCerts.subList(0, 2),
        caCertificateHolder,
        CRLReason.unspecified, securityConfig,
        keyPair);

    // This should create a CRL with sequence id INITIAL_SEQUENCE_ID + 2
    // And contain 2 certificates in it
    iter = scmMetadataStore.getCRLInfoTable().iterator();
    iter.seekToLast();
    Assert.assertEquals(INITIAL_SEQUENCE_ID + 2L, (long) iter.key());

    // Check the sequence ID table for latest sequence id
    Assert.assertEquals(INITIAL_SEQUENCE_ID + 2L, (long)
        scmMetadataStore.getCRLSequenceIdTable().get(CRL_SEQUENCE_ID_KEY));

    CRLInfo newCrlInfo = iter.value().getValue();
    revokedCertificates = newCrlInfo.getX509CRL().getRevokedCertificates();
    Assert.assertEquals(2L, revokedCertificates.size());
    Assert.assertNotNull(
        revokedCertificates.stream().filter(c ->
            c.getSerialNumber().equals(newCerts.get(0).getSerialNumber()))
            .findAny());

    Assert.assertNotNull(
        revokedCertificates.stream().filter(c ->
            c.getSerialNumber().equals(newCerts.get(1).getSerialNumber()))
            .findAny());

    // Valid certs table should have 1 cert
    size = 0;
    TableIterator<BigInteger,
        ? extends Table.KeyValue<BigInteger, X509Certificate>> iterator =
        scmMetadataStore.getValidCertsTable().iterator();
    while (iterator.hasNext()) {
      iterator.next();
      size++;
    }
    Assert.assertEquals(1L, size);
    // Make sure that the last certificate that was not revoked is the one
    // in the valid certs table.
    Assert.assertEquals(newCerts.get(2).getSerialNumber(),
        scmMetadataStore.getValidCertsTable().iterator().next().getKey());

    // Revoked certs table should have 3 certs
    size = 0;
    Iterator iterator1 = scmMetadataStore.getRevokedCertsTable().iterator();
    while (iterator1.hasNext()) {
      size++;
      iterator1.next();
    }
    Assert.assertEquals(3L, size);
  }

  private X509Certificate generateX509Cert(KeyPair keypair) throws Exception {
    if (keypair == null) {
      keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    }
    return CertificateCodec.getX509Certificate(
        CertificateCodec.getPEMEncodedString(
            KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
        "SHA256withRSA")));
  }
}