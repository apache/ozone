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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.security.exception.OzoneSecurityException;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.hadoop.hdds.security.exception.OzoneSecurityException.ResultCodes.OM_PUBLIC_PRIVATE_KEY_FILE_NOT_EXIST;

/**
 * Certificate client provides and interface to certificate operations that
 * needs to be performed by all clients in the Ozone eco-system.
 */
public interface CertificateClient extends Closeable {

  /**
   * Return component name of this certificate client.
   * @return component name
   */
  String getComponentName();

  /**
   * Returns the private key of the specified component if it exists on the
   * local system.
   *
   * @return private key or Null if there is no data.
   */
  PrivateKey getPrivateKey();

  /**
   * Returns the public key of the specified component if it exists on the local
   * system.
   *
   * @return public key or Null if there is no data.
   */
  PublicKey getPublicKey();

  /**
   * Returns the certificate used by the specified component if it exists
   * on the local system.
   *
   * @return the target certificate or null if there is no data.
   */
  X509Certificate getCertificate();

  /**
   * Returns the certificate  of the specified component if it exists on the
   * local system.
   * @param certSerialId
   *
   * @return certificate or Null if there is no data.
   */
  X509Certificate getCertificate(String certSerialId)
      throws CertificateException;

  /**
   * Returns the full certificate path of the specified component if it
   * exists on the local system.
   *
   * @return certificate or Null if there is no data.
   */
  CertPath getCertPath();

  /**
   * Return the latest CA certificate known to the client.
   *
   * @return latest ca certificate known to the client.
   */
  X509Certificate getCACertificate();

  /**
   * Return all certificates in this component's trust chain,
   * the last one is the root CA certificate.
   */
  List<X509Certificate> getTrustChain() throws IOException;

  /**
   * Return the latest Root CA certificate known to the client.
   *
   * @return latest Root CA certificate known to the client.
   */
  X509Certificate getRootCACertificate();

  /**
   * Return the root ca certs saved in this client's file system.
   *
   * @return all the Root CA certificates known to the client
   */
  Set<X509Certificate> getAllRootCaCerts();

  /**
   * Return the subordinate ca certs saved in this client's file system.
   *
   * @return all the subordinate CA certificates known to the client
   */
  Set<X509Certificate> getAllCaCerts();

  /**
   * Return the pem encoded CA certificate list.
   * <p>
   * If initialized return list of pem encoded CA certificates, else return
   * null.
   *
   * @return list of pem encoded CA certificates.
   */
  List<String> getCAList();

  /**
   * Return the pem encoded  CA certificate list.
   *
   * If list is null, fetch the list from SCM and returns the list.
   * If list is not null, return the pem encoded  CA certificate list.
   *
   * @return list of pem encoded  CA certificates.
   * @throws IOException
   */
  List<String> listCA() throws IOException;

  /**
   * Update and returns the pem encoded CA certificate list.
   * @return list of pem encoded  CA certificates.
   * @throws IOException
   */
  List<String> updateCAList() throws IOException;

  /**
   * Creates digital signature over the data stream using the components private
   * key.
   *
   * @param data data to be signed
   * @return byte array - containing the signature
   * @throws CertificateException - on Error
   */
  byte[] signData(byte[] data) throws CertificateException;

  /**
   * Verifies a digital Signature, given the signature and the certificate of
   * the signer.
   * @param data - Data in byte array.
   * @param signature - Byte Array containing the signature.
   * @param cert - Certificate of the Signer.
   * @return true if verified, false if not.
   */
  boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException;

  /**
   * Returns a CSR builder that can be used to create a Certificate sigining
   * request.
   *
   * @return CertificateSignRequest.Builder
   */
  CertificateSignRequest.Builder getCSRBuilder()
      throws CertificateException;

  /**
   * Send request to SCM to sign the certificate and save certificates returned
   * by SCM to PEM files on disk.
   *
   * @return the serial ID of the new certificate
   */
  String signAndStoreCertificate(PKCS10CertificationRequest request)
      throws CertificateException;

  /**
   * Stores the Certificate  for this client. Don't use this api to add
   * trusted certificates of others.
   *
   * @param pemEncodedCert - pem encoded X509 Certificate
   * @param caType         - Is CA certificate.
   * @throws CertificateException - on Error.
   */
  void storeCertificate(String pemEncodedCert, CAType caType)
      throws CertificateException;

  default void assertValidKeysAndCertificate() throws OzoneSecurityException {
    try {
      Objects.requireNonNull(getPublicKey());
      Objects.requireNonNull(getPrivateKey());
      Objects.requireNonNull(getCertificate());
    } catch (Exception e) {
      throw new OzoneSecurityException("Error reading keypair & certificate", e,
          OM_PUBLIC_PRIVATE_KEY_FILE_NOT_EXIST);
    }
  }

  /**
   * Return the store factory for key manager and trust manager for server.
   */
  KeyStoresFactory getServerKeyStoresFactory() throws CertificateException;

  /**
   * Return the store factory for key manager and trust manager for client.
   */
  KeyStoresFactory getClientKeyStoresFactory() throws CertificateException;

  /**
   * Register a receiver that will be called after the certificate renewed.
   *
   * @param receiver
   */
  void registerNotificationReceiver(CertificateNotification receiver);

  /**
   * Registers a listener that will be notified if the CA certificates are
   * changed.
   *
   * @param listener the listener to call with the actualized list of CA
   *                 certificates.
   */
  void registerRootCARotationListener(
      Function<List<X509Certificate>, CompletableFuture<Void>> listener);

  /**
   * Initialize certificate client.
   *
   * */
  void initWithRecovery() throws IOException;

  /**
   * Represents initialization response of client.
   * 1. SUCCESS: Means client is initialized successfully and all required
   *              files are in expected state.
   * 2. FAILURE: Initialization failed due to some unrecoverable error.
   * 3. GETCERT: Bootstrap of keypair is successful but certificate is not
   *             found. Client should request SCM signed certificate.
   *
   */
  enum InitResponse {
    SUCCESS,
    FAILURE,
    GETCERT
  }
}
