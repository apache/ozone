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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.CRL_SEQUENCE_ID_KEY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore.CertType.VALID_CERTS;

/**
 * A Certificate Store class that persists certificates issued by SCM CA.
 */
public class SCMCertStore implements CertificateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertStore.class);
  private SCMMetadataStore scmMetadataStore;
  private final Lock lock;

  public SCMCertStore(SCMMetadataStore dbStore) {
    this.scmMetadataStore = dbStore;
    lock = new ReentrantLock();

  }

  @Override
  public void storeValidCertificate(BigInteger serialID,
      X509Certificate certificate, NodeType role)
      throws IOException {
    lock.lock();
    try {
      // This makes sure that no certificate IDs are reusable.
      checkValidCertID(serialID);
      if (role == SCM) {
        // If the role is SCM, store certificate in scm cert table
        // and valid cert table. This is to help to return scm certs during
        // getCertificate call.
        storeValidScmCertificate(serialID, certificate);
      } else {
        // As we don't have different table for other roles, other role
        // certificates will go to validCertsTable.
        scmMetadataStore.getValidCertsTable().put(serialID, certificate);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Writes a new SCM certificate that was issued to the persistent store.
   * @param serialID - Certificate Serial Number.
   * @param certificate - Certificate to persist.
   * @throws IOException - on Failure.
   */
  private void storeValidScmCertificate(BigInteger serialID,
      X509Certificate certificate) throws IOException {
    lock.lock();
    try {
      checkValidCertID(serialID);
      BatchOperation batchOperation =
          scmMetadataStore.getBatchHandler().initBatchOperation();
      scmMetadataStore.getValidSCMCertsTable().putWithBatch(batchOperation,
          serialID, certificate);
      scmMetadataStore.getValidCertsTable().putWithBatch(batchOperation,
          serialID, certificate);
      scmMetadataStore.getStore().commitBatchOperation(batchOperation);
    } finally {
      lock.unlock();
    }
  }

  private void checkValidCertID(BigInteger serialID) throws IOException {
    if ((getCertificateByID(serialID, VALID_CERTS) != null) ||
        (getCertificateByID(serialID, CertType.REVOKED_CERTS) != null)) {
      throw new SCMSecurityException("Conflicting certificate ID");
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
        if (scmMetadataStore.getValidSCMCertsTable().get(serialID) != null) {
          scmMetadataStore.getValidSCMCertsTable().deleteWithBatch(batch,
              serialID);
        }
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
    if (certType == VALID_CERTS) {
      return scmMetadataStore.getValidCertsTable().get(serialID);
    } else {
      return scmMetadataStore.getRevokedCertsTable().get(serialID);
    }
  }

  @Override
  public List<X509Certificate> listCertificate(NodeType role,
      BigInteger startSerialID, int count, CertType certType)
      throws IOException {

    Preconditions.checkNotNull(startSerialID);

    if (startSerialID.longValue() == 0) {
      startSerialID = null;
    }

    List<? extends Table.KeyValue<BigInteger, X509Certificate>> certs =
        getCertTableList(role, certType, startSerialID, count);

    List<X509Certificate> results = new ArrayList<>(certs.size());

    for (Table.KeyValue<BigInteger, X509Certificate> kv : certs) {
      try {
        X509Certificate cert = kv.getValue();
        results.add(cert);
      } catch (IOException e) {
        LOG.error("Fail to list certificate from SCM metadata store", e);
        throw new SCMSecurityException(
            "Fail to list certificate from SCM metadata store.");
      }
    }
    return results;
  }

  private List<? extends Table.KeyValue<BigInteger, X509Certificate>>
      getCertTableList(NodeType role, CertType certType,
      BigInteger startSerialID, int count)
      throws IOException {
    // Implemented for role SCM and CertType VALID_CERTS.
    // TODO: Implement for role OM/Datanode and for SCM for CertType
    //  REVOKED_CERTS.

    if (role == SCM) {
      if (certType == VALID_CERTS) {
        return scmMetadataStore.getValidSCMCertsTable().getRangeKVs(
            startSerialID, count);
      } else {
        return scmMetadataStore.getRevokedCertsTable().getRangeKVs(
            startSerialID, count);
      }
    } else {
      if (certType == VALID_CERTS) {
        return scmMetadataStore.getValidCertsTable().getRangeKVs(
            startSerialID, count);
      } else {
        return scmMetadataStore.getRevokedCertsTable().getRangeKVs(
            startSerialID, count);
      }
    }
  }

  /**
   * Reinitialise the underlying store with SCMMetaStore
   * during SCM StateMachine reload.
   * @param metadataStore
   */
  @Override
  public void reinitialize(SCMMetadataStore metadataStore) {
    this.scmMetadataStore = metadataStore;
  }
}
