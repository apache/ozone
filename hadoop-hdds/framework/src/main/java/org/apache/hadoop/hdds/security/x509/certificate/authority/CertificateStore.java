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
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;

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
  @Replicate(invocationType = Replicate.InvocationType.CLIENT)
  void storeValidCertificate(BigInteger serialID,
      X509Certificate certificate, NodeType role)
      throws IOException;

  void storeValidScmCertificate(BigInteger serialID,
      X509Certificate certificate) throws IOException;

  /**
   * Check certificate serialID exists or not. If exists throws an exception.
   * @param serialID
   * @throws IOException
   */
  void checkValidCertID(BigInteger serialID) throws IOException;

  /**
   * Deletes all expired certificates from the store.
   *
   * @return The list of removed expired certificates
   * @throws IOException - on failure
   */
  @Replicate
  List<X509Certificate> removeAllExpiredCertificates() throws IOException;

  /**
   * Retrieves a Certificate based on the Serial number of that certificate.
   *
   * @param serialID - ID of the certificate.
   * @return X509Certificate
   * @throws IOException - on failure.
   */
  X509Certificate getCertificateByID(BigInteger serialID) throws IOException;

  /**
   * @param role          - role of the certificate owner (OM/DN).
   * @param startSerialID - start cert serial id.
   * @param count         - max number of certs returned.
   * @return list of X509 certificates.
   * @throws IOException - on failure.
   */
  List<X509Certificate> listCertificate(NodeType role, BigInteger startSerialID, int count) throws IOException;

  /**
   * Reinitialize the certificate server.
   * @param metadataStore SCMMetaStore.
   */
  void reinitialize(SCMMetadataStore metadataStore);

}
