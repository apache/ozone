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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test Class for Root Certificate generation.
 */
public class TestRootCertificate {
  private static final String BASIC_CONSTRAINTS_EXTENSION_OID = "2.5.29.19";

  private SecurityConfig securityConfig;

  @BeforeEach
  public void init(@TempDir Path tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testAllFieldsAreExpected() throws Exception {
    ZonedDateTime notBefore = ZonedDateTime.now();
    ZonedDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setKey(keyPair)
            .setConfiguration(securityConfig);

    X509Certificate certificate = builder.build();

    //Assert that we indeed have a self-signed certificate.
    assertEquals(certificate.getIssuerDN(), certificate.getSubjectDN());


    // Make sure that NotBefore is before the current Date
    Date invalidDate = Date.from(notBefore.minusDays(1).toInstant());
    assertFalse(certificate.getNotBefore().before(invalidDate));

    //Make sure the end date is honored.
    invalidDate = Date.from(notAfter.plusDays(1).toInstant());
    assertFalse(certificate.getNotAfter().after(invalidDate));

    // Check the Subject Name and Issuer Name is in the expected format.
    // Note that the X500Principal class correctly applies RFC-4512 on distinguished names, and returns the RDNs in
    // reverse order as defined in 2.1 in RFC-4512.
    String dnName = String.format("SERIALNUMBER=%s, O=%s, OU=%s, CN=%s",
        certificate.getSerialNumber(), clusterID, scmID, subject);
    assertEquals(dnName, certificate.getIssuerDN().toString());
    assertEquals(dnName, certificate.getSubjectDN().toString());

    assertEquals(-1, certificate.getBasicConstraints(), "Non-CA cert contains the CA flag in BasicConstraints.");

    // Extract the Certificate and verify that certificate matches the public key.
    certificate.verify(keyPair.getPublic());
  }

  @Test
  public void testCACert(@TempDir Path basePath) throws Exception {
    ZonedDateTime notBefore = ZonedDateTime.now();
    ZonedDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    X509Certificate certificate =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setKey(keyPair)
            .setConfiguration(securityConfig)
            .makeCA()
            .addInetAddresses()
            .build();

    assertNotEquals(-1, certificate.getBasicConstraints(), "CA cert does not contain the CA flag in BasicConstraints.");
    assertTrue(certificate.getCriticalExtensionOIDs().contains(BASIC_CONSTRAINTS_EXTENSION_OID));

    // Since this code assigns ONE for the root certificate, we check if the
    // serial number is the expected number.
    assertEquals(BigInteger.ONE, certificate.getSerialNumber());

    CertificateCodec codec = new CertificateCodec(securityConfig, "scm");
    String pemString = CertificateCodec.getPEMEncodedString(certificate);

    codec.writeCertificate(basePath, "pemcertificate.crt",
        pemString);

    X509Certificate loadedCert = codec.getTargetCert(basePath, "pemcertificate.crt");
    assertNotNull(loadedCert);
    assertEquals(certificate.getSerialNumber(),
        loadedCert.getSerialNumber());
  }

  @Test
  public void testInvalidParamFails() throws Exception {
    ZonedDateTime notBefore = ZonedDateTime.now();
    ZonedDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig);
    KeyPair keyPair = keyGen.generateKey();

    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setConfiguration(securityConfig)
            .setKey(keyPair)
            .makeCA();
    try {
      builder.setKey(null);
      builder.build();
      fail("Null Key should have failed.");
    } catch (NullPointerException | IllegalArgumentException e) {
      builder.setKey(keyPair);
    }

    // Now try with Blank Subject.
    assertThrows(IllegalArgumentException.class, () -> {
      builder.setSubject("");
      builder.build();
    });
    builder.setSubject(subject);

    // Now try with blank/null SCM ID
    assertThrows(IllegalArgumentException.class, () -> {
      builder.setScmID(null);
      builder.build();
    });
    builder.setScmID(scmID);

    // Now try with blank/null SCM ID
    assertThrows(IllegalArgumentException.class, () -> {
      builder.setClusterID(null);
      builder.build();
    });
    builder.setClusterID(clusterID);

    // Swap the Begin and End Date and verify that we cannot create a
    // certificate like that.
    assertThrows(IllegalArgumentException.class, () -> {
      builder.setBeginDate(notAfter);
      builder.setEndDate(notBefore);
      builder.build();
    });
    builder.setBeginDate(notBefore);
    builder.setEndDate(notAfter);

    try {
      KeyPair newKey = keyGen.generateKey();
      KeyPair wrongKey = new KeyPair(newKey.getPublic(), keyPair.getPrivate());
      builder.setKey(wrongKey);
      X509Certificate cert = builder.build();
      cert.verify(wrongKey.getPublic());
      fail("Invalid Key, should have thrown.");
    } catch (SCMSecurityException | CertificateException
        | SignatureException | InvalidKeyException e) {
      builder.setKey(keyPair);
    }
    // Assert that we can create a certificate with all sane params.
    assertNotNull(builder.build());
  }
}
