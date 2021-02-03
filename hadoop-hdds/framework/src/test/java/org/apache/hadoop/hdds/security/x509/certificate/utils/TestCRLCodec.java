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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificates.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CRLEntryHolder;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests for the CRLCodec.
 */
public class TestCRLCodec {

  private static OzoneConfiguration conf = new OzoneConfiguration();
  private static final String COMPONENT = "test";
  private SecurityConfig securityConfig;
  private X509CertificateHolder x509CertificateHolder;
  private KeyPair keyPair;
  private static final String TMP_CERT_FILE_NAME = "pemcertificate.crt";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File basePath;
  private static final String TMP_CRL_ENTRY =
      "-----BEGIN X509 CRL-----\n" +
      "MIIBijB0AgEBMA0GCSqGSIb3DQEBCwUAMC0xDTALBgNVBAMMBHdxVG0xDTALBgNV\n" +
      "BAsMBGVFY2gxDTALBgNVBAoMBHJpc1UXDTIwMDExNzE4NTcyMFowIjAgAgEBFw0y\n" +
      "MDAxMTcxODU3MjBaMAwwCgYDVR0VBAMKAQIwDQYJKoZIhvcNAQELBQADggEBACRI\n" +
      "i/nFK2/5rsNWAsYjT/Byhq6shQy+EjdvSzs2cezHbO2TKXnIhlHbvTp5JO/ClaGm\n" +
      "yfdwH6OjQbujcjceSKGSDQwNm98/JsryUh17IWcKJa9dlqFSUCy7GTZaXK6a3nH8\n" +
      "SNhcqzrR69lLc4vJZAy0FkmBCnjbdUX8I92ZHfNQNJaC4JQ8JFtjfzZCcQR9KZxw\n" +
      "bVue37JByiTxmxoiiMZf3MpOccuWKsZzIr9Tiw9G9inPS8lxRXODruDtMTpR8NPB\n" +
      "KL0Yg+JEV48v2GJ5kSObuawCD2uDDNpHDd6q2m1z6J69z5IYpWb8OHEyQT7J4u+b\n" +
      "tPiRCAUQLW9BACm17xc=\n" +
      "-----END X509 CRL-----\n";

  @Before
  public void init() throws NoSuchProviderException,
      NoSuchAlgorithmException, IOException,
      CertificateException, OperatorCreationException {

    conf.set(OZONE_METADATA_DIRS, temporaryFolder.newFolder().toString());
    securityConfig = new SecurityConfig(conf);
    writeTempCert();
    x509CertificateHolder = readTempCert();
  }


  @Test
  public void testWriteCRL() throws IOException, OperatorCreationException {

    X500Name issuer = x509CertificateHolder.getIssuer();
    Date now = new Date();
    X509v2CRLBuilder builder = new X509v2CRLBuilder(issuer, now);
    builder.addCRLEntry(x509CertificateHolder.getSerialNumber(), now,
                        CRLReason.cACompromise);

    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder(securityConfig.getSignatureAlgo());

    contentSignerBuilder.setProvider(securityConfig.getProvider());
    PrivateKey privateKey = keyPair.getPrivate();
    X509CRLHolder cRLHolder =
        builder.build(contentSignerBuilder.build(privateKey));

    CRLCodec crlCodec = new CRLCodec(securityConfig);
    crlCodec.writeCRL(cRLHolder, this.securityConfig.getCrlName(), true);

    X509CRLEntryHolder entryHolder =
        cRLHolder.getRevokedCertificate(BigInteger.ONE);
    assertNotNull(entryHolder);

    // verify file generation
    File crlFile =
        Paths.get(crlCodec.getLocation().toString(),
                  this.securityConfig.getCrlName()).toFile();
    assertTrue(crlFile.exists());

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(crlFile), UTF_8))){

      // Verify contents of the file
      String header = reader.readLine();
      assertEquals("-----BEGIN X509 CRL-----", header);

      String footer = null;
      String line = null;
      while ((line = reader.readLine()) != null) {
        footer = line;
      }
      assertEquals("-----END X509 CRL-----", footer);
    }
  }

  @Test
  public void testWriteCRLX509() throws IOException,
      OperatorCreationException, CertificateException, CRLException {

    X500Name issuer = x509CertificateHolder.getIssuer();
    Date now = new Date();
    X509v2CRLBuilder builder = new X509v2CRLBuilder(issuer, now);
    builder.addCRLEntry(x509CertificateHolder.getSerialNumber(), now,
                        CRLReason.cACompromise);

    byte[] crlBytes = TMP_CRL_ENTRY.getBytes(UTF_8);
    try (InputStream inStream = new ByteArrayInputStream(crlBytes)) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509CRL crl = (X509CRL)cf.generateCRL(inStream);

      CRLCodec crlCodec = new CRLCodec(securityConfig);
      crlCodec.writeCRL(crl);

      // verify file generated or not
      File crlFile =
          Paths.get(crlCodec.getLocation().toString(),
                    this.securityConfig.getCrlName()).toFile();

      assertTrue(crlFile.exists());
    }
  }

  @Test
  public void testGetX509CRL() throws IOException,
      OperatorCreationException, CertificateException, CRLException {

    X500Name issuer = x509CertificateHolder.getIssuer();
    Date now = new Date();
    X509v2CRLBuilder builder = new X509v2CRLBuilder(issuer, now);
    builder.addCRLEntry(x509CertificateHolder.getSerialNumber(), now,
                        CRLReason.cACompromise);

    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder(securityConfig.getSignatureAlgo());

    contentSignerBuilder.setProvider(securityConfig.getProvider());
    PrivateKey privateKey = keyPair.getPrivate();
    X509CRLHolder cRLHolder =
        builder.build(contentSignerBuilder.build(privateKey));

    CRLCodec crlCodec = new CRLCodec(securityConfig);
    crlCodec.writeCRL(cRLHolder, this.securityConfig.getCrlName(), true);

    X509CRLEntryHolder entryHolder =
        cRLHolder.getRevokedCertificate(BigInteger.ONE);
    assertNotNull(entryHolder);

    String pemEncodedString = crlCodec.getPEMEncodedString(cRLHolder);
    assertNotNull(pemEncodedString);

    // Verify header and footer of PEM encoded String
    String header = "-----BEGIN X509 CRL-----";
    String footer = "-----END X509 CRL-----";
    assertTrue(pemEncodedString.contains(header));
    assertTrue(pemEncodedString.contains(footer));
  }

  @Test
  public void testGetX509CRLFromCRLHolder() throws IOException,
      OperatorCreationException, CertificateException, CRLException {

    X500Name issuer = x509CertificateHolder.getIssuer();
    Date now = new Date();
    X509v2CRLBuilder builder = new X509v2CRLBuilder(issuer, now);
    builder.addCRLEntry(x509CertificateHolder.getSerialNumber(), now,
                        CRLReason.cACompromise);

    JcaContentSignerBuilder contentSignerBuilder =
        new JcaContentSignerBuilder(securityConfig.getSignatureAlgo());

    contentSignerBuilder.setProvider(securityConfig.getProvider());
    PrivateKey privateKey = keyPair.getPrivate();
    X509CRLHolder cRLHolder =
        builder.build(contentSignerBuilder.build(privateKey));

    CRLCodec crlCodec = new CRLCodec(securityConfig);

    X509CRL crl = crlCodec.getX509CRL(cRLHolder);
    assertNotNull(crl);
  }

  /**
   * Test method for generating temporary cert and persisting into tmp folder.
   *
   * @throws NoSuchProviderException
   * @throws NoSuchAlgorithmException
   * @throws IOException
   */
  private void writeTempCert() throws NoSuchProviderException,
      NoSuchAlgorithmException, IOException {
    HDDSKeyGenerator keyGenerator =
        new HDDSKeyGenerator(conf);
    keyPair = keyGenerator.generateKey();
    X509CertificateHolder cert =
        SelfSignedCertificate.newBuilder()
            .setSubject(RandomStringUtils.randomAlphabetic(4))
            .setClusterID(RandomStringUtils.randomAlphabetic(4))
            .setScmID(RandomStringUtils.randomAlphabetic(4))
            .setBeginDate(LocalDate.now())
            .setEndDate(LocalDate.now().plus(1, ChronoUnit.DAYS))
            .setConfiguration(keyGenerator.getSecurityConfig()
                                  .getConfiguration())
            .setKey(keyPair)
            .makeCA()
            .build();
    CertificateCodec codec =
        new CertificateCodec(securityConfig, COMPONENT);

    String pemString = codec.getPEMEncodedString(cert);
    basePath = new File(
        String.valueOf(
            securityConfig.getCertificateLocation("scm")));

    if (!basePath.exists()) {
      Assert.assertTrue(basePath.mkdirs());
    }
    codec.writeCertificate(basePath.toPath(), TMP_CERT_FILE_NAME,
                           pemString, false);
  }

  private X509CertificateHolder readTempCert()
      throws IOException, CertificateException {

    CertificateCodec codec =
        new CertificateCodec(securityConfig, COMPONENT);

    X509CertificateHolder x509CertHolder =
        codec.readCertificate(basePath.toPath(), TMP_CERT_FILE_NAME);

    assertNotNull(x509CertHolder);

    return x509CertHolder;
  }
}
