/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.datanode.metadata;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultCRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for {@link DatanodeCRLStoreImpl}.
 */
public class TestDatanodeCRLStoreImpl {
  private File testDir;
  private OzoneConfiguration conf;
  private DatanodeCRLStore dnCRLStore;
  private KeyPair keyPair;
  private CRLApprover crlApprover;
  private SecurityConfig securityConfig;

  @Before
  public void setUp() throws Exception {
    testDir = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    dnCRLStore = new DatanodeCRLStoreImpl(conf);
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    securityConfig = new SecurityConfig(conf);
  }

  @Before
  public void initCRLApprover() {
    crlApprover = new DefaultCRLApprover(securityConfig,
        keyPair.getPrivate());
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(testDir);
  }

  @After
  public void destroyDbStore() throws Exception {
    if (dnCRLStore.getStore() != null) {
      dnCRLStore.getStore().close();
    }
  }
  @Test
  public void testCRLStore() throws Exception {
    assertNotNull(dnCRLStore.getStore());

    dnCRLStore.getCRLSequenceIdTable().put(OzoneConsts.CRL_SEQUENCE_ID_KEY, 5L);
    Date now = new Date();
    X509Certificate x509Certificate = generateX509Cert();
    X509CertificateHolder caCertificateHolder =
        new X509CertificateHolder(generateX509Cert().getEncoded());
    X509v2CRLBuilder crlBuilder = new X509v2CRLBuilder(
        caCertificateHolder.getIssuer(), now);
    crlBuilder.addCRLEntry(x509Certificate.getSerialNumber(), now,
        CRLReason.lookup(CRLReason.PRIVILEGE_WITHDRAWN).getValue().intValue());
    dnCRLStore.getPendingCRLsTable().put(1L,
        new CRLInfo.Builder()
            .setCrlSequenceID(1L)
            .setCreationTimestamp(now.getTime())
            .setX509CRL(crlApprover.sign(crlBuilder))
            .build());

    assertEquals(5L, (long) dnCRLStore.getLatestCRLSequenceID());
    assertEquals(1L, dnCRLStore.getPendingCRLs().size());
    CRLInfo crlInfo = dnCRLStore.getPendingCRLs().get(0);
    assertEquals(1L, crlInfo.getCrlSequenceID());
    assertEquals(x509Certificate.getSerialNumber(),
        crlInfo.getX509CRL().getRevokedCertificates()
            .iterator().next().getSerialNumber());

    // Test that restarting the store does not affect the data already persisted
    dnCRLStore.stop();
    dnCRLStore = new DatanodeCRLStoreImpl(conf);
    assertEquals(5L, (long) dnCRLStore.getLatestCRLSequenceID());
    assertEquals(1L, dnCRLStore.getPendingCRLs().size());
    dnCRLStore.stop();
  }

  private X509Certificate generateX509Cert() throws Exception {
    return CertificateCodec.getX509Certificate(
        CertificateCodec.getPEMEncodedString(
            KeyStoreTestUtil.generateCertificate("CN=Test", keyPair, 30,
                "SHA256withRSA")));
  }
}

