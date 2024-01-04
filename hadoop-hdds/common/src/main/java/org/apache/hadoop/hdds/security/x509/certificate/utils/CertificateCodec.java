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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jcajce.provider.asymmetric.x509.CertificateFactory;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.PEM_ENCODE_FAILED;

/**
 * A class used to read and write X.509 certificates  PEM encoded Streams.
 */
public class CertificateCodec {
  public static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
  public static final String END_CERT = "-----END CERTIFICATE-----";
  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateCodec.class);
  private static final JcaX509CertificateConverter CERTIFICATE_CONVERTER
      = new JcaX509CertificateConverter();
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
   * Returns a X509 Certificate from the Certificate Holder.
   *
   * @param holder - Holder
   * @return X509Certificate.
   * @throws CertificateException - on Error.
   */
  public static X509Certificate getX509Certificate(X509CertificateHolder holder)
      throws CertificateException {
    return CERTIFICATE_CONVERTER.getCertificate(holder);
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
   * Returns the Certificate as a PEM encoded String.
   *
   * @param x509CertHolder - X.509 Certificate Holder.
   * @return PEM Encoded Certificate String.
   * @throws SCMSecurityException - On failure to create a PEM String.
   */
  public static String getPEMEncodedString(X509CertificateHolder x509CertHolder)
      throws SCMSecurityException {
    try {
      return getPEMEncodedString(getX509Certificate(x509CertHolder));
    } catch (CertificateException exp) {
      throw new SCMSecurityException(exp);
    }
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
      LOG.error("Error in encoding certificate." + certificate
          .getSubjectDN().toString(), e);
      throw new SCMSecurityException("PEM Encoding failed for certificate." +
          certificate.getSubjectDN().toString(), e, PEM_ENCODE_FAILED);
    }
  }

  /**
   * Get the leading X.509 Certificate from PEM encoded String possibly
   * containing multiple certificates. To get all certificates, use
   * {@link #getCertPathFromPemEncodedString(String)}.
   *
   * @param pemEncodedString - PEM encoded String.
   * @return X509Certificate  - Certificate.
   * @throws CertificateException - Thrown on Failure.
   */
  public static X509Certificate getX509Certificate(String pemEncodedString)
      throws CertificateException {
    return getX509Certificate(pemEncodedString, Function.identity());
  }

  public static <E extends Exception> X509Certificate getX509Certificate(
      String pemEncoded, Function<CertificateException, E> convertor)
      throws E {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    final ByteArrayInputStream input = new ByteArrayInputStream(
        pemEncoded.getBytes(DEFAULT_CHARSET));
    return readX509Certificate(input, convertor);
  }

  private static <E extends Exception> X509Certificate readX509Certificate(
      InputStream input, Function<CertificateException, E> convertor)
      throws E {
    final CertificateFactory fact = getCertFactory();
    try {
      return (X509Certificate) fact.engineGenerateCertificate(input);
    } catch (CertificateException e) {
      throw convertor.apply(e);
    }
  }

  public static X509Certificate readX509Certificate(InputStream input)
      throws IOException {
    return readX509Certificate(input, CertificateCodec::toIOException);
  }

  public static IOException toIOException(CertificateException e) {
    return new IOException("Failed to engineGenerateCertificate", e);
  }

  public static X509Certificate firstCertificateFrom(CertPath certificatePath) {
    return (X509Certificate) certificatePath.getCertificates().get(0);
  }

  public static CertificateFactory getCertFactory() {
    return new CertificateFactory();
  }

  /**
   * Get Certificate location.
   *
   * @return Path
   */
  public Path getLocation() {
    return location;
  }

  /**
   * Write the Certificate pointed to the location by the configs.
   *
   * @param xCertificate - Certificate to write.
   * @throws SCMSecurityException - on Error.
   * @throws IOException          - on Error.
   */
  public void writeCertificate(X509CertificateHolder xCertificate)
      throws SCMSecurityException, IOException {
    String pem = getPEMEncodedString(xCertificate);
    writeCertificate(location.toAbsolutePath(),
        this.securityConfig.getCertificateFileName(), pem);
  }

  /**
   * Write the Certificate to the specific file.
   *
   * @param xCertificate - Certificate to write.
   * @param fileName     - file name to write to.
   * @throws SCMSecurityException - On Error.
   * @throws IOException          - On Error.
   */
  public void writeCertificate(X509CertificateHolder xCertificate,
      String fileName) throws IOException {
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

    try (FileOutputStream file = new FileOutputStream(certificateFile)) {
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
    try (FileInputStream is = new FileInputStream(certFile)) {
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
   * Returns the Certificate holder from X509Certificate class.
   *
   * @param x509cert - Certificate class.
   * @return X509CertificateHolder
   * @throws CertificateEncodingException - on Error.
   * @throws IOException                  - on Error.
   */
  public static X509CertificateHolder getCertificateHolder(
      X509Certificate x509cert)
      throws CertificateEncodingException, IOException {
    return new X509CertificateHolder(x509cert.getEncoded());
  }

  /**
   * Helper method that takes in a certificate path and a certificate and
   * generates a new certificate path starting with the new certificate
   * followed by all certificates in the specified path.
   */
  public CertPath prependCertToCertPath(X509CertificateHolder certHolder,
      CertPath path) throws CertificateException {
    List<? extends Certificate> certificates = path.getCertificates();
    ArrayList<X509Certificate> updatedList = new ArrayList<>();
    updatedList.add(getX509Certificate(certHolder));
    for (Certificate cert : certificates) {
      updatedList.add((X509Certificate) cert);
    }
    CertificateFactory factory = getCertFactory();
    return factory.engineGenerateCertPath(updatedList);
  }

  /**
   * Helper method that gets one certificate from the specified location.
   * The remaining certificates are ignored.
   */
  public X509CertificateHolder getTargetCertHolder(Path path,
      String fileName) throws CertificateException, IOException {
    CertPath certPath = getCertPath(path, fileName);
    X509Certificate certificate = firstCertificateFrom(certPath);
    return getCertificateHolder(certificate);
  }

  /**
   * Helper method that gets one certificate from the default location.
   * The remaining certificates are ignored.
   */
  public X509CertificateHolder getTargetCertHolder()
      throws CertificateException, IOException {
    return getTargetCertHolder(
        location, securityConfig.getCertificateFileName());
  }

  private static CertPath generateCertPathFromInputStream(
      InputStream inputStream) throws CertificateException {
    CertificateFactory fact = getCertFactory();
    return fact.engineGenerateCertPath(inputStream, "PEM");
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
