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

import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class MockCAStore implements CertificateStore {
  @Override
  public void storeValidCertificate(BigInteger serialID,
      X509Certificate certificate, NodeType role)
      throws IOException {

  }

  @Override
  public void checkValidCertID(BigInteger serialID) throws IOException {
  }

  @Override
  public void storeValidScmCertificate(BigInteger serialID,
      X509Certificate certificate) throws IOException {
  }

  @Override
  public Optional<Long> revokeCertificates(
      List<BigInteger> serialIDs,
      X509CertificateHolder caCertificateHolder,
      CRLReason reason,
      Date revocationTime,
      CRLApprover approver) throws IOException {
    return Optional.empty();
  }

  @Override
  public void removeExpiredCertificate(BigInteger serialID)
      throws IOException {

  }

  @Override
  public X509Certificate getCertificateByID(BigInteger serialID,
                                            CertType certType)
      throws IOException {
    return null;
  }

  @Override
  public CertInfo getRevokedCertificateInfoByID(BigInteger serialID)
      throws IOException {
    return null;
  }

  @Override
  public List<X509Certificate> listCertificate(NodeType role,
      BigInteger startSerialID, int count, CertType certType)
      throws IOException {
    return Collections.emptyList();
  }

  @Override
  public void reinitialize(SCMMetadataStore metadataStore) {}

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    return Collections.emptyList();
  }

  @Override
  public long getLatestCrlId() {
    return 0;
  }
}
