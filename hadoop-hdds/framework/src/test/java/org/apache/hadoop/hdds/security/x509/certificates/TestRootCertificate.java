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

import org.apache.commons.validator.routines.DomainValidator;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificates.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.security.x509.exceptions.CertificateException.ErrorCode.CSR_ERROR;

/**
 * Test Class for Root Certificate generation.
 */
public class TestRootCertificate {
  private static OzoneConfiguration conf = new OzoneConfiguration();
  private SecurityConfig securityConfig;

  @BeforeEach
  public void init(@TempDir Path tempDir) {
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  @Test
  public void testAllFieldsAreExpected()
      throws SCMSecurityException, NoSuchProviderException,
      NoSuchAlgorithmException, CertificateException,
      SignatureException, InvalidKeyException, IOException {
    LocalDateTime notBefore = LocalDateTime.now();
    LocalDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setKey(keyPair)
            .setConfiguration(conf);

    X509CertificateHolder certificateHolder = builder.build();

    //Assert that we indeed have a self signed certificate.
    Assertions.assertEquals(certificateHolder.getIssuer(),
        certificateHolder.getSubject());


    // Make sure that NotBefore is before the current Date
    Date invalidDate = Date.from(
        notBefore.minusDays(1).atZone(ZoneId.systemDefault()).toInstant());
    Assertions.assertFalse(
        certificateHolder.getNotBefore()
            .before(invalidDate));

    //Make sure the end date is honored.
    invalidDate = Date.from(
        notAfter.plusDays(1).atZone(ZoneId.systemDefault()).toInstant());
    Assertions.assertFalse(
        certificateHolder.getNotAfter()
            .after(invalidDate));

    // Check the Subject Name and Issuer Name is in the expected format.
    String dnName = String.format(SelfSignedCertificate.getNameFormat(),
        subject, scmID, clusterID);
    Assertions.assertEquals(dnName, certificateHolder.getIssuer().toString());
    Assertions.assertEquals(dnName, certificateHolder.getSubject().toString());

    // We did not ask for this Certificate to be a CertificateServer
    // certificate, hence that
    // extension should be null.
    Assertions.assertNull(
        certificateHolder.getExtension(Extension.basicConstraints));

    // Extract the Certificate and verify that certificate matches the public
    // key.
    X509Certificate cert =
        new JcaX509CertificateConverter().getCertificate(certificateHolder);
    cert.verify(keyPair.getPublic());
  }

  @Test
  public void testCACert(@TempDir Path basePath)
      throws SCMSecurityException, NoSuchProviderException,
      NoSuchAlgorithmException, IOException, CertificateException {
    LocalDateTime notBefore = LocalDateTime.now();
    LocalDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setKey(keyPair)
            .setConfiguration(conf)
            .makeCA();

    try {
      DomainValidator validator = DomainValidator.getInstance();
      // Add all valid ips.
      OzoneSecurityUtil.getValidInetsForCurrentHost().forEach(
          ip -> {
            builder.addIpAddress(ip.getHostAddress());
            if (validator.isValid(ip.getCanonicalHostName())) {
              builder.addDnsName(ip.getCanonicalHostName());
            }
          });
    } catch (IOException e) {
      throw new org.apache.hadoop.hdds.security.x509
          .exceptions.CertificateException(
          "Error while adding ip to CA self signed certificate", e,
          CSR_ERROR);
    }

    X509CertificateHolder certificateHolder = builder.build();
    // This time we asked for a CertificateServer Certificate, make sure that
    // extension is
    // present and valid.
    Extension basicExt =
        certificateHolder.getExtension(Extension.basicConstraints);

    Assertions.assertNotNull(basicExt);
    Assertions.assertTrue(basicExt.isCritical());

    // Since this code assigns ONE for the root certificate, we check if the
    // serial number is the expected number.
    Assertions.assertEquals(BigInteger.ONE,
        certificateHolder.getSerialNumber());

    CertificateCodec codec = new CertificateCodec(securityConfig, "scm");
    String pemString = codec.getPEMEncodedString(certificateHolder);

    codec.writeCertificate(basePath, "pemcertificate.crt",
        pemString, false);

    X509CertificateHolder loadedCert =
        codec.readCertificate(basePath, "pemcertificate.crt");
    Assertions.assertNotNull(loadedCert);
    Assertions.assertEquals(certificateHolder.getSerialNumber(),
        loadedCert.getSerialNumber());
  }

  @Test
  public void testInvalidParamFails()
      throws SCMSecurityException, NoSuchProviderException,
      NoSuchAlgorithmException, IOException {
    LocalDateTime notBefore = LocalDateTime.now();
    LocalDateTime notAfter = notBefore.plusYears(1);
    String clusterID = UUID.randomUUID().toString();
    String scmID = UUID.randomUUID().toString();
    String subject = "testRootCert";
    HDDSKeyGenerator keyGen =
        new HDDSKeyGenerator(securityConfig.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();

    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(notBefore)
            .setEndDate(notAfter)
            .setClusterID(clusterID)
            .setScmID(scmID)
            .setSubject(subject)
            .setConfiguration(conf)
            .setKey(keyPair)
            .makeCA();
    try {
      builder.setKey(null);
      builder.build();
      Assertions.fail("Null Key should have failed.");
    } catch (NullPointerException | IllegalArgumentException e) {
      builder.setKey(keyPair);
    }

    // Now try with Blank Subject.
    try {
      builder.setSubject("");
      builder.build();
      Assertions.fail("Null/Blank Subject should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setSubject(subject);
    }

    // Now try with blank/null SCM ID
    try {
      builder.setScmID(null);
      builder.build();
      Assertions.fail("Null/Blank SCM ID should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setScmID(scmID);
    }


    // Now try with blank/null SCM ID
    try {
      builder.setClusterID(null);
      builder.build();
      Assertions.fail("Null/Blank Cluster ID should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setClusterID(clusterID);
    }


    // Swap the Begin and End Date and verify that we cannot create a
    // certificate like that.
    try {
      builder.setBeginDate(notAfter);
      builder.setEndDate(notBefore);
      builder.build();
      Assertions.fail("Illegal dates should have thrown.");
    } catch (IllegalArgumentException e) {
      builder.setBeginDate(notBefore);
      builder.setEndDate(notAfter);
    }

    try {
      KeyPair newKey = keyGen.generateKey();
      KeyPair wrongKey = new KeyPair(newKey.getPublic(), keyPair.getPrivate());
      builder.setKey(wrongKey);
      X509CertificateHolder certificateHolder = builder.build();
      X509Certificate cert =
          new JcaX509CertificateConverter().getCertificate(certificateHolder);
      cert.verify(wrongKey.getPublic());
      Assertions.fail("Invalid Key, should have thrown.");
    } catch (SCMSecurityException | CertificateException
        | SignatureException | InvalidKeyException e) {
      builder.setKey(keyPair);
    }
    // Assert that we can create a certificate with all sane params.
    Assertions.assertNotNull(builder.build());
  }
}
