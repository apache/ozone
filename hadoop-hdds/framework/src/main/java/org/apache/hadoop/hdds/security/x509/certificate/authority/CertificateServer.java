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

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateApprover.ApprovalType;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

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
   * @return X509CertificateHolder - Certificate for this CA.
   * @throws CertificateException - usually thrown if this CA is not
   *                              initialized.
   * @throws IOException          - on Error.
   */
  X509CertificateHolder getCACertificate()
      throws CertificateException, IOException;

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
   * @return A future that will have this certificate when this request is
   * approved.
   * @throws SCMSecurityException - on Error.
   */
  Future<X509CertificateHolder> requestCertificate(
      PKCS10CertificationRequest csr,
      CertificateApprover.ApprovalType type, NodeType role)
      throws SCMSecurityException;


  /**
   * Request a Certificate based on Certificate Signing Request.
   *
   * @param csr - Certificate Signing Request as a PEM encoded String.
   * @param type - An Enum which says what kind of approval process to follow.
   * @param nodeType: OM/SCM/DN
   * @return A future that will have this certificate when this request is
   * approved.
   * @throws SCMSecurityException - on Error.
   */
  Future<X509CertificateHolder> requestCertificate(String csr,
      ApprovalType type, NodeType nodeType) throws IOException;

  /**
   * Revokes a Certificate issued by this CertificateServer.
   *
   * @param serialIDs       - List of serial IDs of Certificates to be revoked.
   * @param reason          - Reason for revocation.
   * @param revocationTime  - Revocation time for the certificates.
   * @return Future that gives a list of certificates that were revoked.
   */
  Future<Optional<Long>> revokeCertificates(
      List<BigInteger> serialIDs,
      CRLReason reason,
      Date revocationTime);

  /**
   * List certificates.
   * @param role            - role: OM/SCM/DN
   * @param startSerialId   - start certificate serial id
   * @param count           - max number of certificates returned in a batch
   * @return List of X509 Certificates.
   * @throws IOException - On Failure
   */
  List<X509Certificate> listCertificate(NodeType role,
      long startSerialId, int count, boolean isRevoked) throws IOException;

  /**
   * Reinitialise the certificate server withe the SCMMetastore during SCM
   * state reload post install db checkpoint.
   * @param scmMetadataStore
   */
  void reinitialize(SCMMetadataStore scmMetadataStore);

  /**
   * Get the CRLInfo based on the CRL Ids.
   * @param crlIds - list of crl ids
   * @return CRLInfo
   * @throws IOException
   */
  List<CRLInfo> getCrls(List<Long> crlIds) throws IOException;

  /**
   * Get the latest CRL id.
   * @return latest CRL id.
   */
  long getLatestCrlId();

  /**
   * Make it explicit what type of CertificateServer we are creating here.
   */
  enum CAType {
    SELF_SIGNED_CA,
    INTERMEDIARY_CA
  }
}
