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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import java.io.IOException;
import java.security.cert.CertPath;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

/**
 * Interface for Certificate Authority. This can be extended to talk to
 * external CAs later or HSMs later.
 */
public interface CertificateServer {
  /**
   * Initialize the Certificate Authority.
   *
   * @param securityConfig - Security Configuration.
   * @param type - The Type of CertificateServer we are creating, we make this
   * explicit so that when we read code it is visible to the users.
   * @throws SCMSecurityException - Throws if the init fails.
   */
  void init(SecurityConfig securityConfig, CAType type)
      throws IOException;

  /**
   * Returns the CA Certificate for this CA.
   *
   * @return X509Certificate - Certificate for this CA.
   * @throws CertificateException - usually thrown if this CA is not
   *                              initialized.
   * @throws IOException          - on Error.
   */
  X509Certificate getCACertificate() throws CertificateException, IOException;

  /**
   * Gets the certificate bundle for the CA certificate of this server.
   * The first element of the list is the CA certificate. The issuer of an
   * element of this list is always the next element of the list. The root CA
   * certificate is the final element.
   *
   * @return the certificate bundle starting with the CA certificate.
   * @throws CertificateException
   * @throws IOException
   */
  CertPath getCaCertPath() throws CertificateException,
      IOException;

  /**
   * Returns the Certificate corresponding to given certificate serial id if
   * exist. Return null if it doesn't exist.
   *
   * @return certSerialId         - Certificate serial id.
   * @throws CertificateException - usually thrown if this CA is not
   *                              initialized.
   * @throws IOException          - on Error.
   */
  X509Certificate getCertificate(String certSerialId)
      throws CertificateException, IOException;

  /**
   * Request a Certificate based on Certificate Signing Request.
   *
   * @param csr  - Certificate Signing Request.
   * @param type - An Enum which says what kind of approval process to follow.
   * @param role : OM/SCM/DN
   * @param certSerialId - New certificate ID
   * @return A future that will have this certificate when this request is
   * approved.
   * @throws SCMSecurityException - on Error.
   */
  Future<CertPath> requestCertificate(
      PKCS10CertificationRequest csr,
      CertificateApprover.ApprovalType type, NodeType role,
      String certSerialId) throws SCMSecurityException;

  /**
   * List certificates.
   *
   * @param role          - role: OM/SCM/DN
   * @param startSerialId - start certificate serial id
   * @param count         - max number of certificates returned in a batch
   * @return List of X509 Certificates.
   * @throws IOException - On Failure
   */
  List<X509Certificate> listCertificate(NodeType role,
      long startSerialId, int count) throws IOException;

  /**
   * Reinitialise the certificate server withe the SCMMetastore during SCM
   * state reload post install db checkpoint.
   * @param scmMetadataStore
   */
  void reinitialize(SCMMetadataStore scmMetadataStore);

}
