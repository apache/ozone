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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
   */
  @Test
  public void testGetPEMEncodedString() throws Exception {
    X509Certificate cert = generateTestCert();
    String pemString = CertificateCodec.getPEMEncodedString(cert);
    assertTrue(pemString.startsWith(CertificateCodec.BEGIN_CERT));
    assertTrue(pemString.endsWith(CertificateCodec.END_CERT + "\n"));

    // Read back the certificate and verify that all the comparisons pass.
    X509Certificate newCert = CertificateCodec.getX509Certificate(pemString);
    assertEquals(cert, newCert);
  }

  /**
   * Test when converting a certificate path to pem encoded string and back
   * we get back the same certificates.
   */
  @Test
  public void testGetPemEncodedStringFromCertPath() throws Exception {
    X509Certificate cert1 = generateTestCert();
    X509Certificate cert2 = generateTestCert();
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");

    CertPath pathToEncode = certificateFactory.generateCertPath(ImmutableList.of(cert1, cert2));
    String encodedPath = CertificateCodec.getPEMEncodedString(pathToEncode);
    CertPath certPathDecoded = CertificateCodec.getCertPathFromPemEncodedString(encodedPath);

    assertEquals(cert1, certPathDecoded.getCertificates().get(0));
    assertEquals(cert2, certPathDecoded.getCertificates().get(1));
  }

  /**
   * Test prepending a new certificate to a cert path.
   */
  @Test
  public void testPrependCertificateToCertPath() throws Exception {
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    X509Certificate initialCert = generateTestCert();
    X509Certificate prependedCert = generateTestCert();
    codec.writeCertificate(initialCert);
    CertPath initialPath = codec.getCertPath();
    CertPath pathWithPrependedCert =
        codec.prependCertToCertPath(prependedCert, initialPath);

    assertEquals(prependedCert, pathWithPrependedCert.getCertificates().get(0));
    assertEquals(initialCert, pathWithPrependedCert.getCertificates().get(1));
  }

  /**
   * tests writing and reading certificates in PEM encoded form.
   */
  @Test
  public void testWriteCertificate(@TempDir Path basePath) throws Exception {
    X509Certificate cert = generateTestCert();
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    String pemString = CertificateCodec.getPEMEncodedString(cert);
    codec.writeCertificate(basePath, "pemcertificate.crt", pemString);

    X509Certificate loadedCertificate = codec.getTargetCert(basePath, "pemcertificate.crt");

    assertNotNull(loadedCertificate);
    assertEquals(cert.getSerialNumber(), loadedCertificate.getSerialNumber());
  }

  /**
   * Tests reading and writing certificates in DER form.
   */
  @Test
  public void testWriteCertificateDefault() throws Exception {
    X509Certificate cert = generateTestCert();
    CertificateCodec codec = new CertificateCodec(securityConfig, COMPONENT);
    codec.writeCertificate(cert);

    X509Certificate loadedCertificate = codec.getTargetCert();

    assertNotNull(loadedCertificate);
    assertEquals(cert.getSerialNumber(), loadedCertificate.getSerialNumber());
  }

  /**
   * Tests writing to non-default certificate file name.
   */
  @Test
  public void writeCertificate2() throws Exception {
    X509Certificate cert = generateTestCert();
    CertificateCodec codec = new CertificateCodec(securityConfig, "ca");
    codec.writeCertificate(cert, "newcert.crt");
    // Rewrite with force support
    codec.writeCertificate(cert, "newcert.crt");

    X509Certificate loadedCertificate = codec.getTargetCert(codec.getLocation(), "newcert.crt");

    assertNotNull(loadedCertificate);
  }

  /**
   * Tests writing a certificate path to file and reading back the certificates.
   */
  @Test
  public void testMultipleCertReadWrite() throws Exception {
    //Given a certificate path of one certificate and another certificate
    X509Certificate certToPrepend = generateTestCert();
    X509Certificate initialCert = generateTestCert();
    assertNotEquals(certToPrepend, initialCert);

    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    CertPath certPath = certificateFactory.generateCertPath(ImmutableList.of(initialCert));

    //When prepending the second one before the first one and reading them back
    CertificateCodec codec = new CertificateCodec(securityConfig, "ca");
    CertPath updatedCertPath = codec.prependCertToCertPath(certToPrepend, certPath);

    String certFileName = "newcert.crt";
    String pemEncodedStrings = CertificateCodec.getPEMEncodedString(updatedCertPath);
    codec.writeCertificate(certFileName, pemEncodedStrings);

    CertPath rereadCertPath = codec.getCertPath(certFileName);

    //Then the two certificates are the same as before
    Certificate rereadPrependedCert = rereadCertPath.getCertificates().get(0);
    Certificate rereadSecondCert = rereadCertPath.getCertificates().get(1);
    assertEquals(rereadPrependedCert, certToPrepend);
    assertEquals(rereadSecondCert, initialCert);
  }

  private X509Certificate generateTestCert() throws Exception {
    HDDSKeyGenerator keyGenerator =
        new HDDSKeyGenerator(securityConfig);
    ZonedDateTime startDate = ZonedDateTime.now();
    ZonedDateTime endDate = startDate.plusDays(1);
    return SelfSignedCertificate.newBuilder()
        .setSubject(RandomStringUtils.secure().nextAlphabetic(4))
        .setClusterID(RandomStringUtils.secure().nextAlphabetic(4))
        .setScmID(RandomStringUtils.secure().nextAlphabetic(4))
        .setBeginDate(startDate)
        .setEndDate(endDate)
        .setConfiguration(securityConfig)
        .setKey(keyGenerator.generateKey())
        .makeCA()
        .build();
  }

}
