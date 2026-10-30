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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.mockito.MockedStatic;

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

  @Test
  public void testAddInetAddressesAddsDnsNameFromCanonicalHostName() throws Exception {
    InetAddress address = mock(InetAddress.class);
    when(address.getHostAddress()).thenReturn("192.0.2.10");
    when(address.getCanonicalHostName()).thenReturn("scm1.lxd");

    CertificateSignRequest.Builder builder = newBuilder();
    builder.addInetAddresses(Collections.singletonList(address));

    PKCS10CertificationRequest csr = builder.build().generateCSR();
    List<GeneralName> sanNames = getSanNames(csr);
    assertEquals(1, countByTag(sanNames, GeneralName.iPAddress));
    assertEquals(1, countByTag(sanNames, GeneralName.dNSName));
    assertTrue(dnsNameValues(sanNames).contains("scm1.lxd"));
  }

  @Test
  public void testAddInetAddressesSkipsIpLiteralAndFallbackFailure() throws Exception {
    InetAddress address = mock(InetAddress.class);
    when(address.getHostAddress()).thenReturn("192.0.2.11");
    when(address.getCanonicalHostName()).thenReturn("10.0.0.5");

    CertificateSignRequest.Builder builder = newBuilder();
    try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class, CALLS_REAL_METHODS)) {
      mockedInetAddress.when(InetAddress::getLocalHost).thenThrow(new UnknownHostException("no localhost"));
      builder.addInetAddresses(Collections.singletonList(address));
    }

    PKCS10CertificationRequest csr = builder.build().generateCSR();
    List<GeneralName> sanNames = getSanNames(csr);
    assertEquals(1, countByTag(sanNames, GeneralName.iPAddress));
    assertEquals(0, countByTag(sanNames, GeneralName.dNSName));
  }

  @Test
  public void testAddInetAddressesFallsBackToLocalHostCanonicalName() throws Exception {
    InetAddress address = mock(InetAddress.class);
    when(address.getHostAddress()).thenReturn("192.0.2.12");
    when(address.getCanonicalHostName()).thenReturn("10.0.0.5");

    InetAddress localHost = mock(InetAddress.class);
    when(localHost.getCanonicalHostName()).thenReturn("fallback1.lxd");

    CertificateSignRequest.Builder builder = newBuilder();
    try (MockedStatic<InetAddress> mockedInetAddress = mockStatic(InetAddress.class, CALLS_REAL_METHODS)) {
      mockedInetAddress.when(InetAddress::getLocalHost).thenReturn(localHost);
      builder.addInetAddresses(Collections.singletonList(address));
    }

    PKCS10CertificationRequest csr = builder.build().generateCSR();
    List<GeneralName> sanNames = getSanNames(csr);
    assertEquals(1, countByTag(sanNames, GeneralName.dNSName));
    assertEquals(Collections.singletonList("fallback1.lxd"), dnsNameValues(sanNames));
  }

  @Test
  public void testAddInetAddressesDeduplicatesDnsNamesCaseInsensitively() throws Exception {
    InetAddress address1 = mock(InetAddress.class);
    when(address1.getHostAddress()).thenReturn("192.0.2.13");
    when(address1.getCanonicalHostName()).thenReturn("SCM1.LXD");

    InetAddress address2 = mock(InetAddress.class);
    when(address2.getHostAddress()).thenReturn("192.0.2.14");
    when(address2.getCanonicalHostName()).thenReturn("scm1.lxd");

    CertificateSignRequest.Builder builder = newBuilder();
    builder.addInetAddresses(Arrays.asList(address1, address2));

    PKCS10CertificationRequest csr = builder.build().generateCSR();
    List<GeneralName> sanNames = getSanNames(csr);
    assertEquals(2, countByTag(sanNames, GeneralName.iPAddress));
    assertEquals(1, countByTag(sanNames, GeneralName.dNSName));
  }

  private CertificateSignRequest.Builder newBuilder() throws Exception {
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "DN001";
    HDDSKeyGenerator keyGen = new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();
    return new CertificateSignRequest.Builder()
        .setSubject(subject)
        .setScmID(scmID)
        .setClusterID(clusterID)
        .setKey(keyPair)
        .setConfiguration(securityConfig);
  }

  private List<GeneralName> getSanNames(PKCS10CertificationRequest csr) throws Exception {
    Extensions extensions = getPkcs9Extensions(csr);
    Extension ext = extensions.getExtension(Extension.subjectAlternativeName);
    return Arrays.asList(GeneralNames.getInstance(ext.getParsedValue()).getNames());
  }

  private long countByTag(List<GeneralName> names, int tag) {
    long count = 0;
    for (GeneralName name : names) {
      if (name.getTagNo() == tag) {
        count++;
      }
    }
    return count;
  }

  private List<String> dnsNameValues(List<GeneralName> names) {
    List<String> values = new ArrayList<>();
    for (GeneralName name : names) {
      if (name.getTagNo() == GeneralName.dNSName) {
        values.add(name.getName().toString());
      }
    }
    return values;
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
