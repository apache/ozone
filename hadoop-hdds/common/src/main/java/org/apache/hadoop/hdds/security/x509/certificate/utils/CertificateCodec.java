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

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.PEM_ENCODE_FAILED;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class used to read and write X.509 certificates  PEM encoded Streams.
 */
public class CertificateCodec {
  public static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
  public static final String END_CERT = "-----END CERTIFICATE-----";
  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateCodec.class);
  private final SecurityConfig securityConfig;
  private final Path location;
  private final Set<PosixFilePermission> permissionSet =
      Stream.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE)
          .collect(Collectors.toSet());

  /**
   * Creates a CertificateCodec with component name.
   *
   * @param config - Security Config.
   * @param component - Component String.
   */
  public CertificateCodec(SecurityConfig config, String component) {
    this.securityConfig = config;
    this.location = securityConfig.getCertificateLocation(component);
  }

  public CertificateCodec(SecurityConfig config, Path certPath) {
    this.securityConfig = config;
    this.location = certPath;
  }

  /**
   * Get a valid pem encoded string for the certification path.
   */
  public static String getPEMEncodedString(CertPath certPath)
      throws SCMSecurityException {
    List<? extends Certificate> certsInPath = certPath.getCertificates();
    ArrayList<String> pemEncodedList = new ArrayList<>(certsInPath.size());
    for (Certificate cert : certsInPath) {
      pemEncodedList.add(getPEMEncodedString((X509Certificate) cert));
    }
    return StringUtils.join(pemEncodedList, "\n");
  }

  /**
   * Encode the given certificate in PEM
   * and then write it out to the given {@link OutputStream}.
   *
   * @param <OUT> The output type.
   */
  public static <OUT extends OutputStream> OUT writePEMEncoded(
      X509Certificate certificate, OUT out) throws IOException {
    writePEMEncoded(certificate, new OutputStreamWriter(out, DEFAULT_CHARSET));
    return out;
  }

  /**
   * Encode the given certificate in PEM
   * and then write it out to the given {@link Writer}.
   *
   * @param <W> The writer type.
   */
  public static <W extends Writer> W writePEMEncoded(
      X509Certificate certificate, W writer) throws IOException {
    try (JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {
      pemWriter.writeObject(certificate);
    }
    return writer;
  }

  /**
   * Returns the Certificate as a PEM encoded String.
   *
   * @param certificate - X.509 Certificate.
   * @return PEM Encoded Certificate String.
   * @throws SCMSecurityException - On failure to create a PEM String.
   */
  public static String getPEMEncodedString(X509Certificate certificate)
      throws SCMSecurityException {
    try {
      return writePEMEncoded(certificate, new StringWriter()).toString();
    } catch (IOException e) {
      throw new SCMSecurityException("Failed to getPEMEncodedString for certificate with subject "
          + certificate.getSubjectDN(), e, PEM_ENCODE_FAILED);
    }
  }

  /**
   * Get the leading X.509 Certificate from PEM encoded String possibly
   * containing multiple certificates. To get all certificates, use
   * {@link #getCertPathFromPemEncodedString(String)}.
   *
   * @param pemEncoded - PEM encoded String.
   * @return X509Certificate  - Certificate.
   * @throws CertificateException - Thrown on Failure.
   */
  public static X509Certificate getX509Certificate(String pemEncoded)
      throws CertificateException {
    return getX509Certificate(pemEncoded.getBytes(DEFAULT_CHARSET));
  }

  public static X509Certificate getX509Certificate(byte[] pemEncoded)
      throws CertificateException {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    final ByteArrayInputStream input = new ByteArrayInputStream(pemEncoded);
    return readX509Certificate(input);
  }

  public static X509Certificate readX509Certificate(InputStream input) throws CertificateException {
    final Certificate cert = getCertFactory().generateCertificate(input);
    if (cert instanceof X509Certificate) {
      return (X509Certificate) cert;
    }
    throw new CertificateException("Certificate is not a X509Certificate: " + cert.getClass() + ", " + cert);
  }

  public static X509Certificate readX509Certificate(String pemEncoded) throws IOException {
    try {
      return getX509Certificate(pemEncoded);
    } catch (CertificateException e) {
      throw new IOException("Failed to getX509Certificate from " + pemEncoded, e);
    }
  }

  public static X509Certificate firstCertificateFrom(CertPath certificatePath) {
    return (X509Certificate) certificatePath.getCertificates().get(0);
  }

  public static CertificateFactory getCertFactory() throws CertificateException {
    try {
      return CertificateFactory.getInstance("X.509", "BC");
    } catch (NoSuchProviderException e) {
      throw new RuntimeException("BouncyCastle JCE provider not loaded.", e);
    }
  }

  /**
   * Get Certificate location.
   *
   * @return Path
   */
  public Path getLocation() {
    return location;
  }

  public void writeCertificate(X509Certificate xCertificate) throws IOException {
    String pem = getPEMEncodedString(xCertificate);
    writeCertificate(location.toAbsolutePath(),
        this.securityConfig.getCertificateFileName(), pem);
  }

  public void writeCertificate(X509Certificate xCertificate, String fileName) throws IOException {
    String pem = getPEMEncodedString(xCertificate);
    writeCertificate(location.toAbsolutePath(), fileName, pem);
  }

  /**
   * Write the pem encoded string to the specified file.
   */
  public void writeCertificate(String fileName, String pemEncodedCert)
      throws IOException {
    writeCertificate(location.toAbsolutePath(), fileName, pemEncodedCert);
  }

  /**
   * Helper function that writes data to the file.
   *
   * @param basePath              - Base Path where the file needs to written
   *                              to.
   * @param fileName              - Certificate file name.
   * @param pemEncodedCertificate - pemEncoded Certificate file.
   * @throws IOException - on Error.
   */
  public synchronized void writeCertificate(Path basePath, String fileName,
      String pemEncodedCertificate)
      throws IOException {
    checkBasePathDirectory(basePath);
    File certificateFile =
        Paths.get(basePath.toString(), fileName).toFile();

    try (OutputStream file = Files.newOutputStream(certificateFile.toPath())) {
      file.write(pemEncodedCertificate.getBytes(DEFAULT_CHARSET));
    }
    LOG.info("Save certificate to {}", certificateFile.getAbsolutePath());
    LOG.info("Certificate {}", pemEncodedCertificate);
    Files.setPosixFilePermissions(certificateFile.toPath(), permissionSet);
  }

  /**
   * Gets a certificate path from the specified pem encoded String.
   */
  public static CertPath getCertPathFromPemEncodedString(
      String pemString) throws CertificateException {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    return generateCertPathFromInputStream(
        new ByteArrayInputStream(pemString.getBytes(DEFAULT_CHARSET)));
  }

  private CertPath getCertPath(Path path, String fileName) throws IOException,
      CertificateException {
    checkBasePathDirectory(path.toAbsolutePath());
    File certFile =
        Paths.get(path.toAbsolutePath().toString(), fileName).toFile();
    if (!certFile.exists()) {
      throw new IOException("Unable to find the requested certificate file. " +
          "Path: " + certFile);
    }
    try (InputStream is = Files.newInputStream(certFile.toPath())) {
      return generateCertPathFromInputStream(is);
    }
  }

  /**
   * Get the certificate path stored under the specified filename.
   */
  public CertPath getCertPath(String fileName)
      throws IOException, CertificateException {
    return getCertPath(location, fileName);
  }

  /**
   * Get the default certificate path for this cert codec.
   */
  public CertPath getCertPath() throws CertificateException, IOException {
    return getCertPath(this.securityConfig.getCertificateFileName());
  }

  /**
   * Helper method that takes in a certificate path and a certificate and
   * generates a new certificate path starting with the new certificate
   * followed by all certificates in the specified path.
   */
  public CertPath prependCertToCertPath(X509Certificate certificate, CertPath path) throws CertificateException {
    List<? extends Certificate> certificates = path.getCertificates();
    ArrayList<X509Certificate> updatedList = new ArrayList<>();
    updatedList.add(certificate);
    for (Certificate cert : certificates) {
      updatedList.add((X509Certificate) cert);
    }
    return getCertFactory().generateCertPath(updatedList);
  }

  /**
   * Helper method that gets one certificate from the specified location.
   * The remaining certificates are ignored.
   */
  public X509Certificate getTargetCert(Path path, String fileName) throws CertificateException, IOException {
    CertPath certPath = getCertPath(path, fileName);
    return firstCertificateFrom(certPath);
  }

  /**
   * Helper method that gets one certificate from the default location.
   * The remaining certificates are ignored.
   */
  public X509Certificate getTargetCert() throws CertificateException, IOException {
    return getTargetCert(
        location, securityConfig.getCertificateFileName());
  }

  private static CertPath generateCertPathFromInputStream(InputStream inputStream) throws CertificateException {
    return getCertFactory().generateCertPath(inputStream, "PEM");
  }

  private void checkBasePathDirectory(Path basePath) throws IOException {
    if (!basePath.toFile().exists()) {
      if (!basePath.toFile().mkdirs()) {
        LOG.error("Unable to create file path. Path: {}", basePath);
        throw new IOException("Creation of the directories failed."
            + basePath);
      }
    }
  }
}
