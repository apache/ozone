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

package org.apache.hadoop.hdds.security.x509.certificate.client;

import static org.apache.hadoop.hdds.security.exception.OzoneSecurityException.ResultCodes.OM_PUBLIC_PRIVATE_KEY_FILE_NOT_EXIST;

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
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.security.exception.OzoneSecurityException;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509KeyManager;
import org.apache.hadoop.hdds.security.ssl.ReloadingX509TrustManager;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;

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
   * Returns a CertificateSignRequest Builder object, that can be used to configure the sign request
   * which we use to get  a signed certificate from our CA server implementation.
   *
   * @return CertificateSignRequest.Builder a {@link CertificateSignRequest}
   *           based on which the certificate may be issued to this client.
   */
  CertificateSignRequest.Builder configureCSRBuilder() throws SCMSecurityException;

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
   * Gets a KeyManager containing this CertificateClient's key material and trustchain.
   * During certificate rotation this KeyManager is automatically updated with the new keys/certificates.
   *
   * @return A KeyManager containing keys and the trustchain for this CertificateClient.
   * @throws CertificateException
   */
  ReloadingX509KeyManager getKeyManager() throws CertificateException;

  /**
   * Gets a TrustManager containing the trusted certificates of this CertificateClient.
   * During certificate rotation this TrustManager is automatically updated with the new certificates.
   *
   * @return A TrustManager containing trusted certificates for this CertificateClient.
   * @throws CertificateException
   */
  ReloadingX509TrustManager getTrustManager() throws CertificateException;

  /**
   * Creates a ClientTrustManager instance using the trusted certificates of this certificate client.
   *
   * @return The new ClientTrustManager instance.
   * @throws IOException
   */
  ClientTrustManager createClientTrustManager() throws IOException;

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
