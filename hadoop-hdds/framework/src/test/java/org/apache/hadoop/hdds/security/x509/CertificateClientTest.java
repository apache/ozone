/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.security.x509;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertStore;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateNotification;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.security.x509.exceptions.CertificateException;

import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

/**
 * Test implementation for CertificateClient. To be used only for test
 * purposes.
 */

public class CertificateClientTest implements CertificateClient {
  private KeyPair keyPair;
  private X509Certificate x509Certificate;
  private SecurityConfig secConfig;

  public CertificateClientTest(OzoneConfiguration conf)
      throws Exception {
    secConfig = new SecurityConfig(conf);
    keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    x509Certificate = KeyStoreTestUtil.generateCertificate("CN=OzoneMaster",
        keyPair, 30, "SHA256withRSA");
  }

  @Override
  public PrivateKey getPrivateKey() {
    return keyPair.getPrivate();
  }

  @Override
  public PublicKey getPublicKey() {
    return keyPair.getPublic();
  }

  /**
   * Returns the certificate  of the specified component if it exists on the
   * local system.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public X509Certificate getCertificate(String certSerialId)
      throws CertificateException {
    return x509Certificate;
  }

  @Override
  public X509Certificate getCertificate() {
    return x509Certificate;
  }

  @Override
  public X509Certificate getCACertificate() {
    return x509Certificate;
  }

  @Override
  public boolean verifyCertificate(X509Certificate certificate) {
    return true;
  }

  @Override
  public void setCertificateId(String certSerialId) {
  }

  @Override
  public byte[] signDataStream(InputStream stream)
      throws CertificateException {
    return new byte[0];
  }

  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    return new byte[0];
  }

  @Override
  public boolean verifySignature(InputStream stream, byte[] signature,
      X509Certificate cert) throws CertificateException {
    return true;
  }

  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    return true;
  }

  @Override
  public CertificateSignRequest.Builder getCSRBuilder(KeyPair key)
      throws IOException {
    return null;
  }

  @Override
  public CertificateSignRequest.Builder getCSRBuilder() {
    return new CertificateSignRequest.Builder();
  }

  @Override
  public String signAndStoreCertificate(PKCS10CertificationRequest request,
      Path certPath) throws CertificateException {
    return null;
  }

  @Override
  public String signAndStoreCertificate(PKCS10CertificationRequest request)
      throws CertificateException {
    return null;
  }

  @Override
  public X509Certificate queryCertificate(String query) {
    return null;
  }

  @Override
  public void storeCertificate(String cert, boolean force)
      throws CertificateException {
  }

  @Override
  public void storeCertificate(String cert, boolean force, boolean caCert)
      throws CertificateException {
  }

  /**
   * Stores the trusted chain of certificates for a specific component.
   *
   * @param keyStore - Cert Store.
   * @throws CertificateException - on Error.
   */
  @Override
  public void storeTrustChain(CertStore keyStore) throws CertificateException {

  }

  @Override
  public void storeTrustChain(List<X509Certificate> certificates)
      throws CertificateException {
  }

  @Override
  public InitResponse init() throws CertificateException {
    return null;
  }

  @Override
  public String getSignatureAlgorithm() {
    return secConfig.getSignatureAlgo();
  }

  @Override
  public String getSecurityProvider() {
    return secConfig.getProvider();
  }

  @Override
  public String getComponentName() {
    return "test";
  }

  @Override
  public X509Certificate getRootCACertificate() {
    return x509Certificate;
  }

  @Override
  public void storeRootCACertificate(String pemEncodedCert, boolean force) {

  }

  @Override
  public List<String> getCAList() {
    return null;
  }
  @Override
  public List<String> listCA() throws IOException  {
    return null;
  }

  @Override
  public List<String> updateCAList() throws IOException  {
    return null;
  }

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public long getLatestCrlId() throws IOException {
    return 0;
  }

  @Override
  public long getLocalCrlId() {
    return 0;
  }

  @Override
  public void setLocalCrlId(long crlId) {
  }

  @Override
  public boolean processCrl(CRLInfo crl) {
    return false;
  }

  @Override
  public KeyStoresFactory getServerKeyStoresFactory()
      throws CertificateException {
    return null;
  }

  @Override
  public KeyStoresFactory getClientKeyStoresFactory()
      throws CertificateException {
    return null;
  }

  @Override
  public void registerNotificationReceiver(CertificateNotification receiver) {
  }

  public void renewKey() throws Exception {
    KeyPair newKeyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate newCert = KeyStoreTestUtil.generateCertificate(
        "CN=OzoneMaster", keyPair, 30, "SHA256withRSA");

    keyPair = newKeyPair;
    x509Certificate = newCert;
  }

  @Override
  public void close() throws IOException {
  }
}
