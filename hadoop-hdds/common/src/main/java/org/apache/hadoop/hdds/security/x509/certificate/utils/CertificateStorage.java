/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.x509.certificate.utils;

import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;

/**
 * Class for storing certificates to disk.
 */
public class CertificateStorage {

  private static final String CERT_FILE_EXTENSION = ".crt";
  public static final String CERT_FILE_NAME_FORMAT = "%s" + CERT_FILE_EXTENSION;

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateStorage.class);

  private static final Set<PosixFilePermission> PERMISSION_SET =
      Stream.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE)
          .collect(Collectors.toSet());

  private final SecurityConfig config;
  private final CertificateCodec certificateCodec;

  public CertificateStorage(SecurityConfig conf) {
    this.config = conf;
    this.certificateCodec = conf.getCertificateCodec();
  }

  /**
   * Helper function that writes data to the file.
   *
   * @param certDir               - Directory where the certificates are stored.
   * @param pemEncodedCertificate - pemEncoded Certificate file.
   * @throws IOException - on Error.
   */
  public synchronized CertPath storeCertificate(Path certDir, String pemEncodedCertificate, CAType caType)
      throws IOException {
    CertPath certPath = certificateCodec.getCertPathFromPemEncodedString(pemEncodedCertificate);
    X509Certificate cert = (X509Certificate) certPath.getCertificates().get(0);
    String certName = String.format(CERT_FILE_NAME_FORMAT,
        caType.getFileNamePrefix() + cert.getSerialNumber().toString());
    Path finalPath = Paths.get(certDir.toAbsolutePath().toString(), certName);
    storeCertificate(finalPath.toAbsolutePath(), pemEncodedCertificate);
    return certPath;
  }

  /**
   * @param pathToCertificate     the path which points to the certificate file after it is written.
   *                              The leaf of the path is the certificate file itself.
   * @param pemEncodedCertificate
   * @throws IOException
   */
  public synchronized void storeCertificate(Path pathToCertificate, String pemEncodedCertificate) throws IOException {
    checkDirectoryExists(pathToCertificate.getParent());
    File certificateFile = pathToCertificate.toFile();
    try (FileOutputStream file = new FileOutputStream(certificateFile)) {
      file.write(pemEncodedCertificate.getBytes(DEFAULT_CHARSET));
    }
    LOG.info("Save certificate to {}", certificateFile.getAbsolutePath());
    LOG.info("Certificate {}", pemEncodedCertificate);
    Files.setPosixFilePermissions(certificateFile.toPath(), PERMISSION_SET);
  }

  public synchronized void storeCertificate(Path pathToCertificate, CertPath certPath) throws IOException {
    storeCertificate(pathToCertificate, certificateCodec.getPEMEncodedString(certPath));
  }

  public synchronized void storeCertificate(Path pathToCertificate, X509Certificate certificate) throws IOException {
    storeCertificate(pathToCertificate, certificateCodec.getPEMEncodedString(certificate));
  }

  public CertPath getCertPath(String componentName, String fileName) throws IOException, CertificateException {
    Path path = config.getCertificateLocation(componentName);
    return getCertPath(path, fileName);
  }

  private CertPath getCertPath(Path certDir, String certFileName) throws IOException, CertificateException {
    checkDirectoryExists(certDir.toAbsolutePath());
    File certFile =
        Paths.get(certDir.toAbsolutePath().toString(), certFileName).toFile();
    if (!certFile.exists()) {
      throw new IOException("Unable to find the requested certificate file. " +
          "Path: " + certFile);
    }
    try (FileInputStream is = new FileInputStream(certFile)) {
      return certificateCodec.generateCertPathFromInputStream(is);
    }
  }

  public X509Certificate getFirstCertFromCertPath(Path certDir, String certFileName)
      throws IOException, CertificateException {
    CertPath certPath = getCertPath(certDir, certFileName);
    return (X509Certificate) certPath.getCertificates().get(0);
  }

  private static void checkDirectoryExists(Path basePath) throws IOException {
    if (!basePath.toFile().exists()) {
      if (!basePath.toFile().mkdirs()) {
        LOG.error("Unable to create file path. Path: {}", basePath);
        throw new IOException("Creation of the directories failed."
            + basePath);
      }
    }
  }
}
