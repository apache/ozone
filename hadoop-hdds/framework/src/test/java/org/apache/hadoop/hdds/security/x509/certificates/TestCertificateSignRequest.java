/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.security.x509.certificates;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
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
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCSException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Certificate Signing Request.
 */
public class TestCertificateSignRequest {

  private static OzoneConfiguration conf = new OzoneConfiguration();
  private SecurityConfig securityConfig;

  @BeforeEach
  public void init(@TempDir Path tempDir) throws IOException {
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testGenerateCSR() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException,
      OperatorCreationException, PKCSException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);
    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assertions.assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assertions.assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assertions.assertEquals(1, csr.getAttributes().length);
    Extensions extensions = SecurityUtil.getPkcs9Extensions(csr);

    // Verify key usage extension
    Extension keyUsageExt = extensions.getExtension(Extension.keyUsage);
    Assertions.assertTrue(keyUsageExt.isCritical());


    // Verify San extension not set
    Assertions.assertNull(
        extensions.getExtension(Extension.subjectAlternativeName));

    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    Assertions.assertTrue(csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRwithSan() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException,
      OperatorCreationException, PKCSException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);

    // Multi-home
    builder.addIpAddress("192.168.1.1");
    builder.addIpAddress("192.168.2.1");
    builder.addServiceName("OzoneMarketingCluster003");

    builder.addDnsName("dn1.abc.com");

    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assertions.assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assertions.assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assertions.assertEquals(1, csr.getAttributes().length);
    Extensions extensions = SecurityUtil.getPkcs9Extensions(csr);

    // Verify key usage extension
    Extension sanExt = extensions.getExtension(Extension.keyUsage);
    Assertions.assertTrue(sanExt.isCritical());

    verifyServiceId(extensions);

    // Verify signature in CSR
    ContentVerifierProvider verifierProvider =
        new JcaContentVerifierProviderBuilder().setProvider(securityConfig
            .getProvider()).build(csr.getSubjectPublicKeyInfo());
    Assertions.assertTrue(csr.isSignatureValid(verifierProvider));
  }

  @Test
  public void testGenerateCSRWithInvalidParams() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);

    try {
      builder.setKey(null);
      builder.build();
      Assertions.fail("Null Key should have failed.");
    } catch (NullPointerException | IllegalArgumentException e) {
      builder.setKey(keyPair);
    }

    // Now try with blank/null Subject.
    try {
      builder.setSubject(null);
      builder.build();
      Assertions.fail("Null/Blank Subject should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setSubject(subject);
    }

    try {
      builder.setSubject("");
      builder.build();
      Assertions.fail("Null/Blank Subject should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setSubject(subject);
    }

    // Now try with invalid IP address
    try {
      builder.addIpAddress("255.255.255.*");
      builder.build();
      Assertions.fail("Invalid ip address");
    } catch (IllegalArgumentException e) {
    }

    PKCS10CertificationRequest csr = builder.build();

    // Check the Subject Name is in the expected format.
    String dnName = String.format(SecurityUtil.getDistinguishedNameFormat(),
        subject, scmID, clusterID);
    Assertions.assertEquals(dnName, csr.getSubject().toString());

    // Verify the public key info match
    byte[] encoded = keyPair.getPublic().getEncoded();
    SubjectPublicKeyInfo subjectPublicKeyInfo =
        SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(encoded));
    SubjectPublicKeyInfo csrPublicKeyInfo = csr.getSubjectPublicKeyInfo();
    Assertions.assertEquals(subjectPublicKeyInfo, csrPublicKeyInfo);

    // Verify CSR with attribute for extensions
    Assertions.assertEquals(1, csr.getAttributes().length);
  }

  @Test
  public void testCsrSerialization() throws NoSuchProviderException,
      NoSuchAlgorithmException, SCMSecurityException, IOException {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    CertificateSignRequest.Builder builder =
        new CertificateSignRequest.Builder()
            .setSubject(subject)
            .setScmID(scmID)
            .setClusterID(clusterID)
            .setKey(keyPair)
            .setConfiguration(conf);

    PKCS10CertificationRequest csr = builder.build();
    byte[] csrBytes = csr.getEncoded();

    // Verify de-serialized CSR matches with the original CSR
    PKCS10CertificationRequest dsCsr = new PKCS10CertificationRequest(csrBytes);
    Assertions.assertEquals(csr, dsCsr);
  }

  private void verifyServiceId(Extensions extensions) {
    GeneralNames gns =
        GeneralNames.fromExtensions(
            extensions, Extension.subjectAlternativeName);
    GeneralName[] names = gns.getNames();
    for (int i = 0; i < names.length; i++) {
      if (names[i].getTagNo() == GeneralName.otherName) {
        ASN1Encodable asn1Encodable = names[i].getName();
        Iterator iterator = ((DLSequence) asn1Encodable).iterator();
        while (iterator.hasNext()) {
          Object o = iterator.next();
          if (o instanceof ASN1ObjectIdentifier) {
            String oid = o.toString();
            Assertions.assertEquals("2.16.840.1.113730.3.1.34", oid);
          }
          if (o instanceof DERTaggedObject) {
            String serviceName = ((DERTaggedObject)o).getObject().toString();
            Assertions.assertEquals("OzoneMarketingCluster003", serviceName);
          }
        }
      }
    }
  }
}
