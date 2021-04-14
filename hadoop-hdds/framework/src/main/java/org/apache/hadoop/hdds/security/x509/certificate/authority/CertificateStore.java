/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.authority;

import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * This interface allows the DefaultCA to be portable and use different DB
 * interfaces later. It also allows us define this interface in the SCM layer
 * by which we don't have to take a circular dependency between hdds-common
 * and the SCM.
 *
 * With this interface, DefaultCA server read and write DB or persistence
 * layer and we can write to SCM's Metadata DB.
 */
public interface CertificateStore {

  /**
   * Writes a new certificate that was issued to the persistent store.
   *
   * Note: Don't rename this method, as it is used in
   * SCMHAInvocationHandler#invokeRatis. If for any case renaming this
   * method name is required, change it over there.
   *
   * @param serialID - Certificate Serial Number.
   * @param certificate - Certificate to persist.
   * @param role - OM/DN/SCM.
   * @throws IOException - on Failure.
   */
  @Replicate
  void storeValidCertificate(BigInteger serialID,
      X509Certificate certificate, NodeType role) throws IOException;

  void storeValidScmCertificate(BigInteger serialID,
      X509Certificate certificate) throws IOException;

  /**
   * Check certificate serialID exists or not. If exists throws an exception.
   * @param serialID
   * @throws IOException
   */
  void checkValidCertID(BigInteger serialID) throws IOException;


  /**
   * Adds the certificates to be revoked to a new CRL and moves all the
   * certificates in a transactional manner from valid certificate to
   * revoked certificate state. Returns an empty {@code Optional} instance if
   * the certificates were invalid / not found / already revoked and no CRL
   * was generated. Otherwise, returns the newly generated CRL sequence ID.
   * @param serialIDs - List of Serial IDs of Certificates to be revoked.
   * @param caCertificateHolder - X509 Certificate Holder of the CA.
   * @param reason - CRLReason for revocation.
   * @param revocationTime - Revocation Time for the certificates.
   * @param approver - CRL approver to sign the CRL.
   * @return An empty {@code Optional} instance if no CRL was generated.
   * Otherwise, returns the newly generated CRL sequence ID.
   * @throws IOException - on failure.
   */
  @Replicate
  Optional<Long> revokeCertificates(List<BigInteger> serialIDs,
                                    X509CertificateHolder caCertificateHolder,
                                    CRLReason reason,
                                    Date revocationTime,
                                    CRLApprover approver)
      throws IOException;

  /**
   * Deletes an expired certificate from the store. Please note: We don't
   * remove revoked certificates, we need that information to generate the
   * CRLs.
   * @param serialID - Certificate ID.
   */
  void removeExpiredCertificate(BigInteger serialID) throws IOException;

  /**
   * Retrieves a Certificate based on the Serial number of that certificate.
   * @param serialID - ID of the certificate.
   * @param certType - Whether its Valid or Revoked certificate.
   * @return X509Certificate
   * @throws IOException - on failure.
   */
  X509Certificate getCertificateByID(BigInteger serialID, CertType certType)
      throws IOException;

  /**
   * Retrieves a {@link CertInfo} for a revoked certificate based on the Serial
   * number of that certificate. This API can be used to get more information
   * like the timestamp when the certificate was persisted in the DB.
   * @param serialID - ID of the certificate.
   * @return CertInfo
   * @throws IOException - on failure.
   */
  CertInfo getRevokedCertificateInfoByID(BigInteger serialID)
      throws IOException;

  /**
   *
   * @param role - role of the certificate owner (OM/DN).
   * @param startSerialID - start cert serial id.
   * @param count - max number of certs returned.
   * @param certType cert type (valid/revoked).
   * @return list of X509 certificates.
   * @throws IOException - on failure.
   */
  List<X509Certificate> listCertificate(NodeType role,
      BigInteger startSerialID, int count, CertType certType)
      throws IOException;

  /**
   * Reinitialize the certificate server.
   * @param metadataStore SCMMetaStore.
   */
  void reinitialize(SCMMetadataStore metadataStore);

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
   * Different kind of Certificate stores.
   */
  enum CertType {
    VALID_CERTS,
    REVOKED_CERTS
  }
}
