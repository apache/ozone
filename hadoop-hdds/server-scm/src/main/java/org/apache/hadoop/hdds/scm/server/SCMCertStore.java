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
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.security.cert.CRLException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.security.x509.crl.CRLStatus;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CRLApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.CRL_SEQUENCE_ID_KEY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType.SCM;
import static org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore.CertType.VALID_CERTS;

/**
 * A Certificate Store class that persists certificates issued by SCM CA.
 */
public final class SCMCertStore implements CertificateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertStore.class);
  private SCMMetadataStore scmMetadataStore;
  private final Lock lock;
  private final AtomicLong crlSequenceId;
  private final Map<UUID, CRLStatus> crlStatusMap;

  private SCMCertStore(SCMMetadataStore dbStore, long sequenceId) {
    this.scmMetadataStore = dbStore;
    lock = new ReentrantLock();
    crlSequenceId = new AtomicLong(sequenceId);
    crlStatusMap = new ConcurrentHashMap<>();
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
    } finally {
      lock.unlock();
    }
  }

  public void checkValidCertID(BigInteger serialID) throws IOException {
    lock.lock();
    try {
      if ((getCertificateByID(serialID, VALID_CERTS) != null) ||
          (getCertificateByID(serialID, CertType.REVOKED_CERTS) != null)) {
        throw new SCMSecurityException("Conflicting certificate ID" + serialID);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<Long> revokeCertificates(
      List<BigInteger> serialIDs,
      X509CertificateHolder caCertificateHolder,
      CRLReason reason,
      Date revocationTime,
      CRLApprover crlApprover)
      throws IOException {
    Date now = new Date();
    X509v2CRLBuilder builder =
        new X509v2CRLBuilder(caCertificateHolder.getIssuer(), now);
    List<X509Certificate> certsToRevoke = new ArrayList<>();
    X509CRL crl;
    Optional<Long> sequenceId = Optional.empty();
    lock.lock();
    try {
      for (BigInteger serialID: serialIDs) {
        X509Certificate cert =
            getCertificateByID(serialID, CertType.VALID_CERTS);
        if (cert == null && LOG.isWarnEnabled()) {
          LOG.warn("Trying to revoke a certificate that is not valid. " +
              "Serial ID: {}", serialID.toString());
        } else if (getCertificateByID(serialID, CertType.REVOKED_CERTS)
            != null) {
          LOG.warn("Trying to revoke a certificate that is already revoked.");
        } else {
          builder.addCRLEntry(serialID, revocationTime,
              reason.getValue().intValue());
          certsToRevoke.add(cert);
        }
      }
      if (!certsToRevoke.isEmpty()) {
        try {
          crl = crlApprover.sign(builder);
        } catch (OperatorCreationException | CRLException e) {
          throw new SCMSecurityException("Unable to create Certificate " +
              "Revocation List.", e);
        }
        // let us do this in a transaction.
        try (BatchOperation batch =
                 scmMetadataStore.getStore().initBatchOperation()) {
          // Move the certificates from Valid Certs table to Revoked Certs Table
          // only if the revocation time has passed.
          if (now.after(revocationTime) || now.equals(revocationTime)) {
            for (X509Certificate cert : certsToRevoke) {
              CertInfo certInfo = new CertInfo.Builder()
                  .setX509Certificate(cert)
                  .setTimestamp(now.getTime())
                  .build();
              scmMetadataStore.getRevokedCertsV2Table()
                  .putWithBatch(batch, cert.getSerialNumber(), certInfo);
              scmMetadataStore.getValidCertsTable()
                  .deleteWithBatch(batch, cert.getSerialNumber());
            }
          }
          long id = crlSequenceId.incrementAndGet();
          CRLInfo crlInfo = new CRLInfo.Builder()
              .setX509CRL(crl)
              .setCreationTimestamp(now.getTime())
              .setCrlSequenceID(id)
              .build();
          scmMetadataStore.getCRLInfoTable().putWithBatch(
              batch, id, crlInfo);

          // Update the CRL Sequence Id Table with the last sequence id.
          scmMetadataStore.getCRLSequenceIdTable().putWithBatch(batch,
              CRL_SEQUENCE_ID_KEY, id);
          scmMetadataStore.getStore().commitBatchOperation(batch);
          sequenceId = Optional.of(id);
        }
      }
    } finally {
      lock.unlock();
    }
    return sequenceId;
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
      CertInfo certInfo = getRevokedCertificateInfoByID(serialID);
      return certInfo != null ? certInfo.getX509Certificate() : null;
    }
  }

  @Override
  public CertInfo getRevokedCertificateInfoByID(BigInteger serialID)
      throws IOException {
    return scmMetadataStore.getRevokedCertsV2Table().get(serialID);
  }

  @Override
  public List<X509Certificate> listCertificate(NodeType role,
      BigInteger startSerialID, int count, CertType certType)
      throws IOException {
    List<X509Certificate> results = new ArrayList<>();
    String errorMessage = "Fail to list certificate from SCM metadata store";
    Preconditions.checkNotNull(startSerialID);

    if (startSerialID.longValue() == 0) {
      startSerialID = null;
    }

    if (certType == VALID_CERTS) {
      List<? extends Table.KeyValue<BigInteger, X509Certificate>> certs =
          getValidCertTableList(role, startSerialID, count);

      for (Table.KeyValue<BigInteger, X509Certificate> kv : certs) {
        try {
          X509Certificate cert = kv.getValue();
          results.add(cert);
        } catch (IOException e) {
          LOG.error(errorMessage, e);
          throw new SCMSecurityException(errorMessage);
        }
      }
    } else {
      List<? extends Table.KeyValue<BigInteger, CertInfo>> certs =
          scmMetadataStore.getRevokedCertsV2Table().getRangeKVs(
          startSerialID, count, null);

      for (Table.KeyValue<BigInteger, CertInfo> kv : certs) {
        try {
          CertInfo certInfo = kv.getValue();
          X509Certificate cert = certInfo != null ?
              certInfo.getX509Certificate() : null;
          results.add(cert);
        } catch (IOException e) {
          LOG.error(errorMessage, e);
          throw new SCMSecurityException(errorMessage);
        }
      }
    }
    return results;
  }

  private List<? extends Table.KeyValue<BigInteger, X509Certificate>>
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
    private long crlSequenceId;
    private SCMRatisServer scmRatisServer;


    public Builder setMetadaStore(SCMMetadataStore scmMetadataStore) {
      this.metadataStore = scmMetadataStore;
      return this;
    }

    public Builder setCRLSequenceId(long sequenceId) {
      this.crlSequenceId = sequenceId;
      return this;
    }

    public Builder setRatisServer(final SCMRatisServer ratisServer) {
      scmRatisServer = ratisServer;
      return this;
    }

    public CertificateStore build() {
      final SCMCertStore scmCertStore = new SCMCertStore(metadataStore,
          crlSequenceId);

      final SCMHAInvocationHandler scmhaInvocationHandler =
          new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.CERT_STORE,
              scmCertStore, scmRatisServer);

      return (CertificateStore) Proxy.newProxyInstance(
          SCMHAInvocationHandler.class.getClassLoader(),
          new Class<?>[]{CertificateStore.class}, scmhaInvocationHandler);

    }
  }

  @Override
  public List<CRLInfo> getCrls(List<Long> crlIds) throws IOException {
    List<CRLInfo> results = new ArrayList<>();
    for (Long crlId : crlIds) {
      try {
        CRLInfo crlInfo =
            scmMetadataStore.getCRLInfoTable().get(crlId);
        results.add(crlInfo);
      } catch (IOException e) {
        LOG.error("Fail to get CRLs from SCM metadata store for crlId: "
            + crlId, e);
        throw new SCMSecurityException("Fail to get CRLs from SCM metadata " +
            "store for crlId: " + crlId, e);
      }
    }
    return results;
  }

  @Override
  public long getLatestCrlId() {
    return crlSequenceId.get();
  }

  @Override
  public CRLStatus getCRLStatusForDN(UUID uuid) {
    return crlStatusMap.get(uuid);
  }

  @Override
  public void setCRLStatusForDN(UUID uuid, CRLStatus crlStatus) {
    crlStatusMap.put(uuid, crlStatus);
  }
}
