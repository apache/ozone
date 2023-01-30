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
package org.apache.hadoop.hdds.security.x509.crl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

/**
 * CRL Codec Utility class used for reading and writing
 * X.509 CRL PEM encoded Streams.
 */
public class CRLCodec {
  private static final Logger LOG =
      LoggerFactory.getLogger(CRLCodec.class);
  private static final JcaX509CRLConverter CRL_CONVERTER
      = new JcaX509CRLConverter();
  private final SecurityConfig securityConfig;
  private final Path location;
  private final Set<PosixFilePermission> permissionSet =
      Stream.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE)
          .collect(Collectors.toSet());

  /**
   * The CRL Codec allows us to encode and decode.
   *
   * @param securityConfig
   */
  public CRLCodec(SecurityConfig securityConfig) {
    this.securityConfig = securityConfig;
    this.location = securityConfig.getCertificateLocation("scm");
  }

  /**
   * Returns a X509 CRL from the CRL Holder.
   *
   * @param holder - Holder
   * @return X509CRL - X509 CRL.
   * @throws CRLException - on Error.
   */
  public static X509CRL getX509CRL(X509CRLHolder holder)
      throws CRLException {
    return CRL_CONVERTER.getCRL(holder);
  }

  /**
   * Returns the Certificate as a PEM encoded String.
   *
   * @param holder - X.509 CRL Holder.
   * @return PEM Encoded Certificate String.
   * @throws SCMSecurityException - On failure to create a PEM String.
   */
  public static String getPEMEncodedString(X509CRLHolder holder)
      throws SCMSecurityException {
    LOG.trace("Getting PEM version of a CRL.");
    try {
      return getPEMEncodedString(getX509CRL(holder));
    } catch (CRLException exp) {
      throw new SCMSecurityException(exp);
    }
  }

  public static String getPEMEncodedString(X509CRL holder)
      throws SCMSecurityException {
    try {
      StringWriter stringWriter = new StringWriter();
      try (JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) {
        pemWriter.writeObject(holder);
      }
      return stringWriter.toString();
    } catch (IOException e) {
      throw new SCMSecurityException("PEM Encoding failed for CRL." +
          holder.getIssuerDN().toString(), e);
    }
  }

  /**
   * Gets the X.509 CRL from PEM encoded String.
   *
   * @param pemEncodedString - PEM encoded String.
   * @return X509CRL  - Crl.
   * @throws CRLException - Thrown on Failure.
   * @throws CertificateException - Thrown on Failure.
   * @throws IOException          - Thrown on Failure.
   */
  public static X509CRL getX509CRL(String pemEncodedString)
      throws CRLException, CertificateException, IOException {
    CertificateFactory fact = CertificateFactory.getInstance("X.509");
    try (InputStream input = IOUtils.toInputStream(pemEncodedString, UTF_8)) {
      return (X509CRL) fact.generateCRL(input);
    }
  }

  /**
   * Get CRL location.
   *
   * @return Path
   */
  public Path getLocation() {
    return location;
  }

  /**
   * Write the CRL pointed to the location by the configs.
   *
   * @param crl - X509CRL CRL to write.
   * @throws IOException          - on Error.
   */
  public void writeCRL(X509CRL crl)
      throws IOException {
    String pem = getPEMEncodedString(crl);
    writeCRL(location.toAbsolutePath(),
             this.securityConfig.getCrlName(), pem, false);
  }

  /**
   * Write the CRL to the specific file.
   *
   * @param crlHolder - CRL to write.
   * @param fileName - file name to write to.
   * @param overwrite - boolean value, true means overwrite an existing
   * crl.
   * @throws IOException          - On Error.
   */
  public void writeCRL(X509CRLHolder crlHolder,
      String fileName, boolean overwrite)
      throws IOException {
    String pem = getPEMEncodedString(crlHolder);
    writeCRL(location.toAbsolutePath(), fileName, pem, overwrite);
  }

  /**
   * Write the CRL to the specific file.
   *
   * @param basePath - Base Path where CRL file to be written.
   * @param fileName - file name of CRL file.
   * @param pemCRLString - PEN Encoded string
   * @param force - boolean value, true means overwrite an existing
   * crl.
   * @throws IOException          - On Error.
   */
  public synchronized void writeCRL(Path basePath, String fileName,
      String pemCRLString, boolean force)
      throws IOException {
    File crlFile =
        Paths.get(basePath.toString(), fileName).toFile();
    if (crlFile.exists() && !force) {
      throw new SCMSecurityException("Specified CRL file already " +
          "exists.Please use force option if you want to overwrite it.");
    }
    // if file exists otherwise, if able to create
    if (!basePath.toFile().exists() && !basePath.toFile().mkdirs()) {
      LOG.error("Unable to create file path. Path: {}", basePath);
      throw new IOException("Creation of the directories failed."
                                + basePath.toString());
    }
    try (FileOutputStream file = new FileOutputStream(crlFile)) {
      IOUtils.write(pemCRLString, file, UTF_8);
    }
    Files.setPosixFilePermissions(crlFile.toPath(), permissionSet);
  }

}
