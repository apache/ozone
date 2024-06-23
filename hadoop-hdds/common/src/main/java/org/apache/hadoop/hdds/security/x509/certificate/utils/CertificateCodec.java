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
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
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

  /**
   * Creates a CertificateCodec with component name.
   */
  public CertificateCodec() {
  }

  /**
   * Get a valid pem encoded string for the certification path.
   */
  public String getPEMEncodedString(CertPath certPath)
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
  public <OUT extends OutputStream> OUT writePEMEncoded(
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
  public <W extends Writer> W writePEMEncoded(
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
  public String getPEMEncodedString(X509Certificate certificate)
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
   * @param pemEncoded - PEM encoded String.
   * @return X509Certificate  - Certificate.
   * @throws IOException - Thrown on Failure.
   */
  public X509Certificate getX509Certificate(String pemEncoded) throws IOException {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    final ByteArrayInputStream input = new ByteArrayInputStream(
        pemEncoded.getBytes(DEFAULT_CHARSET));
    return readX509Certificate(input);
  }

  public X509Certificate readX509Certificate(InputStream input) throws IOException {
    try {
      return (X509Certificate) getCertFactory().generateCertificate(input);
    } catch (CertificateException e) {
      throw new IOException("Failed to engineGenerateCertificate", e);
    }
  }

  private CertificateFactory getCertFactory() throws CertificateException {
    try {
      return CertificateFactory.getInstance("X.509", "BC");
    } catch (NoSuchProviderException e) {
      throw new RuntimeException("BouncyCastle JCE provider not loaded.", e);
    }
  }

  /**
   * Gets a certificate path from the specified pem encoded String.
   */
  public CertPath getCertPathFromPemEncodedString(String pemString) throws IOException {
    // ByteArrayInputStream.close(), which is a noop, can be safely ignored.
    try {
      return generateCertPathFromInputStream(new ByteArrayInputStream(pemString.getBytes(DEFAULT_CHARSET)));
    } catch (CertificateException e) {
      throw new IOException(e);
    }
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

  public CertPath generateCertPathFromInputStream(InputStream inputStream) throws CertificateException {
    return getCertFactory().generateCertPath(inputStream, "PEM");
  }
}
