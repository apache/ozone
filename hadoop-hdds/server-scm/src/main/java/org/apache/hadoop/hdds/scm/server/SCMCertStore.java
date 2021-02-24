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
import java.security.cert.CRLException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;

import java.util.List;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
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

/**
 * A Certificate Store class that persists certificates issued by SCM CA.
 */
public class SCMCertStore implements CertificateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMCertStore.class);
  private final SCMMetadataStore scmMetadataStore;
  private final Lock lock;
  private AtomicLong crlSequenceId;

  public SCMCertStore(SCMMetadataStore dbStore, long sequenceId) {
    this.scmMetadataStore = dbStore;
    lock = new ReentrantLock();
    crlSequenceId = new AtomicLong(sequenceId);
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
              scmMetadataStore.getRevokedCertsTable()
                  .putWithBatch(batch, cert.getSerialNumber(), cert);
              scmMetadataStore.getValidCertsTable()
                  .deleteWithBatch(batch, cert.getSerialNumber());
            }
          }
          long id = crlSequenceId.incrementAndGet();
          CRLInfo crlInfo = new CRLInfo.Builder()
              .setX509CRL(crl)
              .setCreationTimestamp(now.getTime())
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
