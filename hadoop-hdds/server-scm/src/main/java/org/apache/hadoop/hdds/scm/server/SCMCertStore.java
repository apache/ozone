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

package org.apache.hadoop.hdds.scm.server;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Certificate Store class that persists certificates issued by SCM CA.
 */
public class SCMCertStore implements CertificateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertStore.class);
  private final SCMMetadataStore scmMetadataStore;
  private final Lock lock;

  public SCMCertStore(SCMMetadataStore dbStore) {
    this.scmMetadataStore = dbStore;
    lock = new ReentrantLock();

  }

  @Override
  public void storeValidCertificate(BigInteger serialID,
                                    X509Certificate certificate)
      throws IOException {
    lock.lock();
    try {
      // This makes sure that no certificate IDs are reusable.
      if ((getCertificateByID(serialID, CertType.VALID_CERTS) == null) &&
          (getCertificateByID(serialID, CertType.REVOKED_CERTS) == null)) {
        scmMetadataStore.getValidCertsTable().put(serialID, certificate);
      } else {
        throw new SCMSecurityException("Conflicting certificate ID");
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void revokeCertificate(BigInteger serialID) throws IOException {
    lock.lock();
    try {
      X509Certificate cert = getCertificateByID(serialID, CertType.VALID_CERTS);
      if (cert == null) {
        LOG.error("trying to revoke a certificate that is not valid. Serial: " +
            "{}", serialID.toString());
        throw new SCMSecurityException("Trying to revoke an invalid " +
            "certificate.");
      }
      // TODO : Check if we are trying to revoke an expired certificate.

      if (getCertificateByID(serialID, CertType.REVOKED_CERTS) != null) {
        LOG.error("Trying to revoke a certificate that is already revoked.");
        throw new SCMSecurityException("Trying to revoke an already revoked " +
            "certificate.");
      }

      // let is do this in a transaction.
      try (BatchOperation batch =
               scmMetadataStore.getStore().initBatchOperation();) {
        scmMetadataStore.getRevokedCertsTable()
            .putWithBatch(batch, serialID, cert);
        scmMetadataStore.getValidCertsTable().deleteWithBatch(batch, serialID);
        scmMetadataStore.getStore().commitBatchOperation(batch);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void removeExpiredCertificate(BigInteger serialID)
      throws IOException {
    // TODO: Later this allows removal of expired certificates from the system.
  }

  @Override
  public X509Certificate getCertificateByID(BigInteger serialID,
                                            CertType certType)
      throws IOException {
    if (certType == CertType.VALID_CERTS) {
      return scmMetadataStore.getValidCertsTable().get(serialID);
    } else {
      return scmMetadataStore.getRevokedCertsTable().get(serialID);
    }
  }

  @Override
  public List<X509Certificate> listCertificate(HddsProtos.NodeType role,
      BigInteger startSerialID, int count, CertType certType)
      throws IOException {
    // TODO: Filter by role
    List<? extends Table.KeyValue<BigInteger, X509Certificate>> certs;
    if (startSerialID.longValue() == 0) {
      startSerialID = null;
    }
    if (certType == CertType.VALID_CERTS) {
      certs = scmMetadataStore.getValidCertsTable().getRangeKVs(
          startSerialID, count);
    } else {
      certs = scmMetadataStore.getRevokedCertsTable().getRangeKVs(
          startSerialID, count);
    }
    List<X509Certificate> results = new ArrayList<>(certs.size());
    for (Table.KeyValue<BigInteger, X509Certificate> kv : certs) {
      try {
        X509Certificate cert = kv.getValue();
        // TODO: filter certificate based on CN and specified role.
        // This requires change of the approved subject CN format:
        // Subject: O=CID-e66d4728-32bb-4282-9770-351a7e913f07,
        // OU=9a7c4f86-c862-4067-b12c-e7bca51d3dfe, CN=root@98dba189d5f0

        // The new format will look like below that are easier to filter.
        // CN=FQDN/user=root/role=datanode/...
        results.add(cert);
      } catch (IOException e) {
        LOG.error("Fail to list certificate from SCM metadata store", e);
        throw new SCMSecurityException(
            "Fail to list certificate from SCM metadata store.");
      }
    }
    return results;
  }
}
