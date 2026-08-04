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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SequenceIdType;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.OngoingStubbing;

/**
 * Tests for the leaderless SCM bootstrap certificate signing path in
 * {@link SCMSecurityProtocolServer}: the shared {@code isLeaderlessPrimaryScmSigner} predicate
 * and the {@code signAndPersistLeaderlessScmCertificate} issuance method.
 */
public class TestLeaderlessScmCertSigning {

  private static final String REQUESTER = "scm-2";

  @TempDir
  private File testDir;

  private final OzoneConfiguration config = new OzoneConfiguration();
  private final PKCS10CertificationRequest csr = mock(PKCS10CertificationRequest.class);

  private CertPath certPathFor(X509Certificate cert) throws Exception {
    return CertificateCodec.getCertFactory().generateCertPath(Collections.singletonList(cert));
  }

  @Test
  public void testSignAndPersistLeaderlessScmCertificateSucceeds() throws Exception {
    KeyPair keys = CertificateTestUtils.aKeyPair(config);
    BigInteger certSerial = BigInteger.valueOf(101);
    X509Certificate leaf = CertificateTestUtils.createSelfSignedCert(
        keys, "scm1", Duration.ofDays(1), certSerial);
    CertPath certPath = certPathFor(leaf);

    CertificateServer nullStoreRootCa = mock(CertificateServer.class);
    when(nullStoreRootCa.requestCertificate(eq(csr), any(), eq(SCM), eq("101")))
        .thenReturn(CompletableFuture.completedFuture(certPath));
    CertificateStore certificateStore = mock(CertificateStore.class);

    String pem = SCMSecurityProtocolServer.signAndPersistLeaderlessScmCertificate(
        nullStoreRootCa, certificateStore, csr, "101", "scm-node-1");

    X509Certificate decoded = CertificateCodec.getX509Certificate(pem);
    assertEquals(leaf.getSerialNumber(), decoded.getSerialNumber());

    verify(certificateStore).checkValidCertID(certSerial);
    verify(certificateStore).storeValidScmCertificate(certSerial, leaf);
    verify(certificateStore, never()).storeValidCertificate(any(), any(), any());
  }

  @Test
  public void testSignAndPersistLeaderlessScmCertificatePropagatesCollision() throws Exception {
    KeyPair keys = CertificateTestUtils.aKeyPair(config);
    BigInteger certSerial = BigInteger.valueOf(202);
    X509Certificate leaf = CertificateTestUtils.createSelfSignedCert(
        keys, "scm2", Duration.ofDays(1), certSerial);
    CertPath certPath = certPathFor(leaf);

    CertificateServer nullStoreRootCa = mock(CertificateServer.class);
    when(nullStoreRootCa.requestCertificate(eq(csr), any(), eq(SCM), eq("202")))
        .thenReturn(CompletableFuture.completedFuture(certPath));
    CertificateStore certificateStore = mock(CertificateStore.class);
    doThrow(new SCMSecurityException("Conflicting certificate ID..."))
        .when(certificateStore).checkValidCertID(certSerial);

    assertThrows(SCMSecurityException.class, () ->
        SCMSecurityProtocolServer.signAndPersistLeaderlessScmCertificate(
            nullStoreRootCa, certificateStore, csr, "202", "scm-node-2"));

    verify(certificateStore, never()).storeValidScmCertificate(any(), any());
  }

  /**
   * Builds a StorageContainerManager mock in the state the predicate admits: this node is the
   * recorded primary SCM and hosts a root CA, the requester ("scm-2") is already a member of the
   * Ratis group. Individual tests override one condition.
   */
  private StorageContainerManager mockScmSatisfyingPredicate() throws Exception {
    StorageContainerManager scm = mock(StorageContainerManager.class, RETURNS_DEEP_STUBS);
    SCMStorageConfig storageConfig = mock(SCMStorageConfig.class);
    when(storageConfig.getPrimaryScmNodeId()).thenReturn("scm-1");
    when(storageConfig.getScmId()).thenReturn("scm-1");
    when(scm.getScmStorageConfig()).thenReturn(storageConfig);
    when(scm.getRootCertificateServer()).thenReturn(mock(CertificateServer.class));
    withRatisPeers(scm, "scm-1", REQUESTER);
    return scm;
  }

  private void withRatisPeers(StorageContainerManager scm, String... peerIds) {
    List<RaftPeer> peers = new ArrayList<>();
    for (String peerId : peerIds) {
      RaftPeer peer = mock(RaftPeer.class);
      when(peer.getId()).thenReturn(RaftPeerId.valueOf(peerId));
      peers.add(peer);
    }
    when(scm.getScmHAManager().getRatisServer().getDivision().getRaftConf().getCurrentPeers())
        .thenReturn(peers);
  }

  /** A certificate shaped like a real SCM certificate: the node id lives in the subject's OU. */
  private X509Certificate scmCertificateFor(String scmId, KeyPair keys) throws Exception {
    return CertificateTestUtils.createSelfSignedCert(keys,
        "scm-sub-ca@localhost,OU=" + scmId + ",O=cluster-1", Duration.ofDays(1));
  }

  /** Stubs the valid SCM certificates table to iterate over the given certificates. */
  @SuppressWarnings("unchecked")
  private void withScmCertificates(StorageContainerManager scm, X509Certificate... certs)
      throws Exception {
    TableIterator<BigInteger, X509Certificate> iterator = mock(TableIterator.class);
    OngoingStubbing<Boolean> hasNext = when(iterator.hasNext());
    for (X509Certificate ignored : certs) {
      hasNext = hasNext.thenReturn(true);
    }
    hasNext.thenReturn(false);
    if (certs.length > 0) {
      when(iterator.next()).thenReturn(certs[0],
          Arrays.copyOfRange(certs, 1, certs.length));
    }
    when(scm.getScmMetadataStore().getValidSCMCertsTable().valueIterator()).thenReturn(iterator);
  }

  private PKCS10CertificationRequest csrFor(KeyPair keys) {
    PKCS10CertificationRequest request = mock(PKCS10CertificationRequest.class);
    when(request.getSubjectPublicKeyInfo())
        .thenReturn(SubjectPublicKeyInfo.getInstance(keys.getPublic().getEncoded()));
    return request;
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerAllConditionsMet() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();

    assertTrue(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenRenewing() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), true, REQUESTER));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenSuggestedLeaderKnown() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(mock(RaftPeer.class));

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, nle, false, REQUESTER));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenNoRootCertificateServer() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    when(scm.getRootCertificateServer()).thenReturn(null);

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenPrimaryScmNodeIdNull() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    when(scm.getScmStorageConfig().getPrimaryScmNodeId()).thenReturn(null);

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenNotPrimaryScm() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    when(scm.getScmStorageConfig().getPrimaryScmNodeId()).thenReturn("scm-9");

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  /**
   * A brand new SCM trying to join a cluster is not yet a Ratis member; it does not predate
   * security and must go through a leader like any other membership change.
   */
  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenRequesterIsNotAnExistingRatisPeer()
      throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    withRatisPeers(scm, "scm-1");

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  /**
   * Once this SCM has been part of a working quorum during its current process, a later election
   * gap is an ordinary NotLeaderException, not a bootstrap.
   */
  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenALeaderHasBeenSeen() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    when(scm.getScmHAManager().getRatisServer().getSCMStateMachine().isLeaderEverKnown())
        .thenReturn(true);

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, noLeaderKnown(), false, REQUESTER));
  }

  /**
   * A peer that crashed before storing the certificate this SCM already issued and persisted asks
   * again with the same key pair; it must get that certificate back rather than a second one.
   */
  @Test
  public void testFindReplayableCertificateSkipsOlderCertificateForSameNode()
      throws Exception {
    KeyPair keys = CertificateTestUtils.aKeyPair(config);
    KeyPair oldKeys = CertificateTestUtils.aKeyPair(config);
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    X509Certificate issued = scmCertificateFor(REQUESTER, keys);
    withScmCertificates(scm, scmCertificateFor("scm-1", CertificateTestUtils.aKeyPair(config)),
        scmCertificateFor(REQUESTER, oldKeys), issued);

    assertEquals(issued,
        SCMSecurityProtocolServer.findReplayableCertificate(scm, REQUESTER, csrFor(keys)));
  }

  /**
   * A peer that lost its key material presents a different public key, so the stored certificate
   * is useless to it and a new one has to be issued.
   */
  @Test
  public void testFindReplayableCertificateIgnoresCertificateForADifferentKey() throws Exception {
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    withScmCertificates(scm, scmCertificateFor(REQUESTER, CertificateTestUtils.aKeyPair(config)));

    assertNull(SCMSecurityProtocolServer.findReplayableCertificate(scm, REQUESTER,
        csrFor(CertificateTestUtils.aKeyPair(config))));
  }

  /**
   * The primary's own certificates share the table; they must not be mistaken for the requester's.
   */
  @Test
  public void testFindReplayableCertificateIgnoresOtherNodesCertificates() throws Exception {
    KeyPair keys = CertificateTestUtils.aKeyPair(config);
    StorageContainerManager scm = mockScmSatisfyingPredicate();
    withScmCertificates(scm, scmCertificateFor("scm-1", keys));

    assertNull(SCMSecurityProtocolServer.findReplayableCertificate(scm, REQUESTER, csrFor(keys)));
  }

  private NotLeaderException noLeaderKnown() {
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(null);
    return nle;
  }

  /**
   * The leaderless path continues the normal CertificateId sequence: each allocate-and-record
   * cycle advances the row by exactly one, so the ids need no reserved sub-range of their own.
   */
  @Test
  public void testNextLeaderlessCertificateIdContinuesTheNormalSequence() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore metadataStore = new SCMMetadataStoreImpl(conf);
    metadataStore.start(conf);
    try {
      Table<SequenceIdType, Long> sequenceIdTable = metadataStore.getSequenceIdTable();
      sequenceIdTable.put(SequenceIdType.CertificateId, 2L);

      for (long expected = 3L; expected <= 5L; expected++) {
        long certId = SCMSecurityProtocolServer.nextLeaderlessCertificateId(metadataStore);
        assertEquals(expected, certId);
        // What getEncodedCertToString() does after signing and persisting the certificate.
        sequenceIdTable.put(SequenceIdType.CertificateId, certId);
      }
    } finally {
      metadataStore.stop();
    }
  }

  @Test
  public void testNextLeaderlessCertificateIdDerivesRowWhenUnset() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    SCMMetadataStore metadataStore = new SCMMetadataStoreImpl(conf);
    metadataStore.start(conf);
    try {
      assertNull(metadataStore.getSequenceIdTable().get(SequenceIdType.CertificateId));

      // Root certificate is id 1 and the primary SCM's own certificate is id 2, so the first
      // certificate this path can issue is 3.
      assertEquals(3L, SCMSecurityProtocolServer.nextLeaderlessCertificateId(metadataStore));
    } finally {
      metadataStore.stop();
    }
  }
}
