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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
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
public final class SCMCertStore implements CertificateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertStore.class);
  private SCMMetadataStore scmMetadataStore;
  private final Lock lock;

  private SCMCertStore(SCMMetadataStore dbStore) {
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
  @Override
  public void storeValidScmCertificate(BigInteger serialID,
      X509Certificate certificate) throws IOException {
    lock.lock();
    try (BatchOperation batchOperation =
             scmMetadataStore.getBatchHandler().initBatchOperation()) {
      scmMetadataStore.getValidSCMCertsTable().putWithBatch(batchOperation,
          serialID, certificate);
      scmMetadataStore.getValidCertsTable().putWithBatch(batchOperation,
          serialID, certificate);
      scmMetadataStore.getStore().commitBatchOperation(batchOperation);
      LOG.info("Scm certificate {} for {} is stored", serialID,
          certificate.getSubjectDN());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void checkValidCertID(BigInteger serialID) throws IOException {
    lock.lock();
    try {
      if (getCertificateByID(serialID) != null) {
        throw new SCMSecurityException("Conflicting certificate ID" + serialID);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<X509Certificate> removeAllExpiredCertificates()
      throws IOException {
    List<X509Certificate> removedCerts = new ArrayList<>();
    lock.lock();
    try (BatchOperation batchOperation =
             scmMetadataStore.getBatchHandler().initBatchOperation()) {
      removedCerts.addAll(addExpiredCertsToBeRemoved(batchOperation,
          scmMetadataStore.getValidCertsTable()));
      removedCerts.addAll(addExpiredCertsToBeRemoved(batchOperation,
          scmMetadataStore.getValidSCMCertsTable()));
      scmMetadataStore.getStore().commitBatchOperation(batchOperation);
    } finally {
      lock.unlock();
    }
    return removedCerts;
  }

  private List<X509Certificate> addExpiredCertsToBeRemoved(
      BatchOperation batchOperation, Table<BigInteger,
      X509Certificate> certTable) throws IOException {
    List<X509Certificate> removedCerts = new ArrayList<>();
    try (Table.KeyValueIterator<BigInteger, X509Certificate> certsIterator = certTable.iterator()) {
      Date now = new Date();
      while (certsIterator.hasNext()) {
        Table.KeyValue<BigInteger, X509Certificate> certEntry =
            certsIterator.next();
        X509Certificate cert = certEntry.getValue();
        if (cert.getNotAfter().before(now)) {
          removedCerts.add(cert);
          certTable.deleteWithBatch(batchOperation, certEntry.getKey());
        }
      }
    }
    return removedCerts;
  }

  @Override
  public X509Certificate getCertificateByID(BigInteger serialID)
      throws IOException {
    return scmMetadataStore.getValidCertsTable().get(serialID);
  }

  @Override
  public List<X509Certificate> listCertificate(NodeType role,
      BigInteger startSerialID, int count)
      throws IOException {
    Objects.requireNonNull(startSerialID, "startSerialID == null");

    if (startSerialID.longValue() == 0) {
      startSerialID = null;
    }

    return getValidCertTableList(role, startSerialID, count).stream()
        .map(Table.KeyValue::getValue)
        .collect(Collectors.toList());
  }

  private List<Table.KeyValue<BigInteger, X509Certificate>>
      getValidCertTableList(NodeType role, BigInteger startSerialID, int count)
      throws IOException {
    // Implemented for role SCM and CertType VALID_CERTS.
    // TODO: Implement for role OM/Datanode

    if (role == SCM) {
      return scmMetadataStore.getValidSCMCertsTable().getRangeKVs(
          startSerialID, count, null);
    } else {
      return scmMetadataStore.getValidCertsTable().getRangeKVs(
          startSerialID, count, null);
    }
  }

  /**
   * Reinitialise the underlying store with SCMMetaStore
   * during SCM StateMachine reload.
   * @param metadataStore SCMMetadataStore
   */
  @Override
  public void reinitialize(SCMMetadataStore metadataStore) {
    this.scmMetadataStore = metadataStore;
  }

  /**
   * Builder for SCMCertStore.
   */
  public static class Builder {

    private SCMMetadataStore metadataStore;
    private SCMRatisServer scmRatisServer;

    public Builder setMetadaStore(SCMMetadataStore scmMetadataStore) {
      this.metadataStore = scmMetadataStore;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public CertificateStore build() {
      final SCMCertStore scmCertStore = new SCMCertStore(metadataStore);
      return scmRatisServer.getProxyHandler(SCMRatisProtocol.RequestType.CERT_STORE,
         CertificateStore.class, scmCertStore);
    }
  }
}
