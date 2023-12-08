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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jcajce.provider.asymmetric.x509.CertificateFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the Certificate codecs.
 */
public class TestCertificateCodec {
  private static final String COMPONENT = "test";
  private SecurityConfig securityConfig;

  @BeforeEach
  public void init(@TempDir Path tempDir) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    securityConfig = new SecurityConfig(conf);
  }

  /**
   * This test converts a X509Certificate Holder object to a PEM encoded String,
   * then creates a new X509Certificate object to verify that we are able to
   * serialize and deserialize correctly. we follow up with converting these
   * objects to standard JCA x509Certificate objects.
   *
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   * @throws SCMSecurityException     - on Error.
   * @throws CertificateException     - on Error.
   */
  @Test
  public void testGetPEMEncodedString()
      throws NoSuchProviderException, NoSuchAlgorithmException,
      IOException, SCMSecurityException, CertificateException {
    X509CertificateHolder cert =
        generateTestCert();
    String pemString = CertificateCodec.getPEMEncodedString(cert);
    assertTrue(pemString.startsWith(CertificateCodec.BEGIN_CERT));
    assertTrue(pemString.endsWith(CertificateCodec.END_CERT + "\n"));

    // Read back the certificate and verify that all the comparisons pass.
    X509CertificateHolder newCert = CertificateCodec.getCertificateHolder(
        CertificateCodec.getX509Certificate(pemString));
    assertEquals(cert, newCert);

    // Just make sure we can decode both these classes to Java Std. lIb classes.
    X509Certificate firstCert = CertificateCodec.getX509Certificate(cert);
    X509Certificate secondCert = CertificateCodec.getX509Certificate(newCert);
    assertEquals(firstCert, secondCert);
  }

  /**
   * Test when converting a certificate path to pem encoded string and back
   * we get back the same certificates.
   */
  @Test
  public void testGetPemEncodedStringFromCertPath() throws IOException,
      NoSuchAlgorithmException, NoSuchProviderException, CertificateException {
    X509CertificateHolder certHolder1 = generateTestCert();
    X509CertificateHolder certHolder2 = generateTestCert();
    X509Certificate cert1 = CertificateCodec.getX509Certificate(certHolder1);
    X509Certificate cert2 = CertificateCodec.getX509Certificate(certHolder2);
    CertificateFactory certificateFactory = new CertificateFactory();
    CertPath pathToEncode =
        certificateFactory.engineGenerateCertPath(ImmutableList.of(cert1,
            cert2));
    String encodedPath = CertificateCodec.getPEMEncodedString(pathToEncode);
    CertPath certPathDecoded =
        CertificateCodec.getCertPathFromPemEncodedString(encodedPath);
    assertEquals(cert1, certPathDecoded.getCertificates().get(0));
    assertEquals(cert2, certPathDecoded.getCertificates().get(1));
  }

  /**
   * Test prepending a new certificate to a cert path.
   */
  @Test
  public void testPrependCertificateToCertPath() throws IOException,
      NoSuchAlgorithmException, NoSuchProviderException, CertificateException {
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    X509CertificateHolder initialHolder = generateTestCert();
    X509CertificateHolder prependedHolder = generateTestCert();
    X509Certificate initialCert =
        CertificateCodec.getX509Certificate(initialHolder);
    X509Certificate prependedCert =
        CertificateCodec.getX509Certificate(prependedHolder);
    codec.writeCertificate(initialHolder);
    CertPath initialPath = codec.getCertPath();
    CertPath pathWithPrependedCert =
        codec.prependCertToCertPath(prependedHolder, initialPath);

    assertEquals(prependedCert, pathWithPrependedCert.getCertificates().get(0));
    assertEquals(initialCert, pathWithPrependedCert.getCertificates().get(1));
  }

  /**
   * tests writing and reading certificates in PEM encoded form.
   *
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws IOException              - on Error.
   * @throws SCMSecurityException     - on Error.
   * @throws CertificateException     - on Error.
   */
  @Test
  public void testWriteCertificate(@TempDir Path basePath)
      throws NoSuchProviderException, NoSuchAlgorithmException,
      IOException, SCMSecurityException, CertificateException {
    X509CertificateHolder cert =
        generateTestCert();
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    String pemString = CertificateCodec.getPEMEncodedString(cert);
    codec.writeCertificate(basePath, "pemcertificate.crt",
        pemString);
    X509CertificateHolder certHolder =
        codec.getTargetCertHolder(basePath, "pemcertificate.crt");
    assertNotNull(certHolder);
    assertEquals(cert.getSerialNumber(),
        certHolder.getSerialNumber());
  }

  /**
   * Tests reading and writing certificates in DER form.
   *
   * @throws IOException              - on Error.
   * @throws SCMSecurityException     - on Error.
   * @throws CertificateException     - on Error.
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   */
  @Test
  public void testWriteCertificateDefault()
      throws IOException, SCMSecurityException, CertificateException,
      NoSuchProviderException, NoSuchAlgorithmException {
    X509CertificateHolder cert = generateTestCert();
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    codec.writeCertificate(cert);
    X509CertificateHolder certHolder = codec.getTargetCertHolder();
    assertNotNull(certHolder);
    assertEquals(cert.getSerialNumber(), certHolder.getSerialNumber());
  }

  /**
   * Tests writing to non-default certificate file name.
   *
   * @throws IOException              - on Error.
   * @throws SCMSecurityException     - on Error.
   * @throws NoSuchProviderException  - on Error.
   * @throws NoSuchAlgorithmException - on Error.
   * @throws CertificateException     - on Error.
   */
  @Test
  public void writeCertificate2() throws IOException, SCMSecurityException,
      NoSuchProviderException, NoSuchAlgorithmException, CertificateException {
    X509CertificateHolder cert = generateTestCert();
    CertificateCodec codec =
        new CertificateCodec(securityConfig, "ca");
    codec.writeCertificate(cert, "newcert.crt");
    // Rewrite with force support
    codec.writeCertificate(cert, "newcert.crt");
    X509CertificateHolder x509CertificateHolder =
        codec.getTargetCertHolder(codec.getLocation(), "newcert.crt");
    assertNotNull(x509CertificateHolder);
  }

  /**
   * Tests writing a certificate path to file and reading back the certificates.
   *
   * @throws IOException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchProviderException
   * @throws CertificateException
   */
  @Test
  public void testMultipleCertReadWrite() throws IOException,
      NoSuchAlgorithmException, NoSuchProviderException, CertificateException {
    //Given a certificate path of one certificate and another certificate
    X509CertificateHolder certToPrepend =
        generateTestCert();

    X509CertificateHolder initialCert =
        generateTestCert();
    assertNotEquals(certToPrepend, initialCert);

    CertificateFactory certificateFactory = new CertificateFactory();
    CertPath certPath =
        certificateFactory.engineGenerateCertPath(
            ImmutableList.of(CertificateCodec.getX509Certificate(initialCert)));

    //When prepending the second one before the first one and reading them back
    CertificateCodec codec =
        new CertificateCodec(securityConfig, "ca");

    CertPath updatedCertPath =
        codec.prependCertToCertPath(certToPrepend, certPath);

    String certFileName = "newcert.crt";
    String pemEncodedStrings =
        CertificateCodec.getPEMEncodedString(updatedCertPath);
    codec.writeCertificate(certFileName, pemEncodedStrings);

    CertPath rereadCertPath =
        codec.getCertPath(certFileName);

    //Then the two certificates are the same as before
    Certificate rereadPrependedCert = rereadCertPath.getCertificates().get(0);
    Certificate rereadSecondCert = rereadCertPath.getCertificates().get(1);
    assertEquals(CertificateCodec.getCertificateHolder(
        (X509Certificate) rereadPrependedCert), certToPrepend);
    assertEquals(CertificateCodec.getCertificateHolder(
        (X509Certificate) rereadSecondCert), initialCert);
  }

  private X509CertificateHolder generateTestCert()
      throws IOException, NoSuchProviderException, NoSuchAlgorithmException {
    HDDSKeyGenerator keyGenerator =
        new HDDSKeyGenerator(securityConfig);
    LocalDateTime startDate = LocalDateTime.now();
    LocalDateTime endDate = startDate.plusDays(1);
    return SelfSignedCertificate.newBuilder()
        .setSubject(RandomStringUtils.randomAlphabetic(4))
        .setClusterID(RandomStringUtils.randomAlphabetic(4))
        .setScmID(RandomStringUtils.randomAlphabetic(4))
        .setBeginDate(startDate)
        .setEndDate(endDate)
        .setConfiguration(securityConfig)
        .setKey(keyGenerator.generateKey())
        .makeCA()
        .build();
  }

}
