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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getDistinguishedNameFormat;
import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest.getPkcs9Extensions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.DERTaggedObject;
import org.bouncycastle.asn1.DLSequence;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.operator.ContentVerifierProvider;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Certificate Signing Request.
 */
public class TestCertificateSignRequest {

  private SecurityConfig securityConfig;

  @BeforeEach
  public void init(@TempDir Path tempDir) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testGenerateCSR() throws Exception {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(securityConfig);
    //TODO: generateCSR!
    PKCS10CertificationRequest csr = builder.build().generateCSR();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    assertEquals(1, csr.getAttributes().length);
    Extensions extensions = getPkcs9Extensions(csr);

    // Verify key usage extension
    Extension keyUsageExt = extensions.getExtension(Extension.keyUsage);
    assertTrue(keyUsageExt.isCritical());


    // Verify San extension not set
    assertNull(extensions.getExtension(Extension.subjectAlternativeName));

    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    assertTrue(csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRwithSan() throws Exception {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(securityConfig);

    // Multi-home
    builder.addIpAddress("192.168.1.1");
    builder.addIpAddress("192.168.2.1");
    builder.addServiceName("OzoneMarketingCluster003");

    builder.addDnsName("dn1.abc.com");

    //TODO: generateCSR!
    PKCS10CertificationRequest csr = builder.build().generateCSR();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    assertEquals(1, csr.getAttributes().length);
    Extensions extensions = getPkcs9Extensions(csr);

    // Verify key usage extension
    Extension sanExt = extensions.getExtension(Extension.keyUsage);
    assertTrue(sanExt.isCritical());

    verifyServiceId(extensions);

    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    assertTrue(csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRWithInvalidParams() throws Exception {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(securityConfig);

    try {
      builder.setKey(null);
      builder.build();
      fail("Null Key should have failed.");
    } catch (NullPointerException | IllegalArgumentException e) {
      builder.setKey(keyPair);
    }

    // Now try with blank/null Subject.
    assertThrows(IllegalArgumentException.class, () -> {
      builder.setSubject(null);
      builder.build();
    });
    builder.setSubject(subject);

    assertThrows(IllegalArgumentException.class, () -> {
      builder.setSubject("");
      builder.build();
    });
    builder.setSubject(subject);

    // Now try with invalid IP address
    assertThrows(IllegalArgumentException.class, () -> {
      builder.addIpAddress("255.255.255.*");
      builder.build();
    });

    //TODO: generateCSR!
    PKCS10CertificationRequest csr = builder.build().generateCSR();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    assertEquals(1, csr.getAttributes().length);
  }

  @Test
  public void testCsrSerialization() throws Exception {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(securityConfig);

    //TODO: generateCSR!
    PKCS10CertificationRequest csr = builder.build().generateCSR();
    byte[] csrBytes = csr.getEncoded();

    // Verify de-serialized CSR matches with the original CSR
    PKCS10CertificationRequest dsCsr = new PKCS10CertificationRequest(csrBytes);
    assertEquals(csr, dsCsr);
  }

  private void verifyServiceId(Extensions extensions) {
    GeneralNames gns =
        GeneralNames.fromExtensions(
            extensions, Extension.subjectAlternativeName);
    GeneralName[] names = gns.getNames();
    for (GeneralName name : names) {
      if (name.getTagNo() == GeneralName.otherName) {
        ASN1Encodable asn1Encodable = name.getName();

        for (Object sequence : (DLSequence) asn1Encodable) {
          if (sequence instanceof ASN1ObjectIdentifier) {
            String oid = sequence.toString();
            assertEquals("2.16.840.1.113730.3.1.34", oid);
          }
          if (sequence instanceof DERTaggedObject) {
            String serviceName = ((DERTaggedObject) sequence).toASN1Primitive().toString();
            assertEquals("OzoneMarketingCluster003", serviceName);
          }
        }
      }
    }
  }
}
