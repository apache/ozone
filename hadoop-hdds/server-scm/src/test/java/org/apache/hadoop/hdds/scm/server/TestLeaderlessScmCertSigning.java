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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateServer;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.Test;

/**
 * Tests for the leaderless SCM bootstrap certificate signing path in
 * {@link SCMSecurityProtocolServer}: the shared {@code isLeaderlessPrimaryScmSigner} predicate
 * and the {@code signAndPersistLeaderlessScmCertificate} issuance method.
 */
public class TestLeaderlessScmCertSigning {

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

  @Test
  public void testIsLeaderlessPrimaryScmSignerAllConditionsMet() {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getRootCertificateServer()).thenReturn(mock(CertificateServer.class));
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(null);

    assertTrue(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, nle, false));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenRenewing() {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getRootCertificateServer()).thenReturn(mock(CertificateServer.class));
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(null);

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, nle, true));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenSuggestedLeaderKnown() {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getRootCertificateServer()).thenReturn(mock(CertificateServer.class));
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(mock(RaftPeer.class));

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, nle, false));
  }

  @Test
  public void testIsLeaderlessPrimaryScmSignerFalseWhenNoRootCertificateServer() {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    when(scm.getRootCertificateServer()).thenReturn(null);
    NotLeaderException nle = mock(NotLeaderException.class);
    when(nle.getSuggestedLeader()).thenReturn(null);

    assertFalse(SCMSecurityProtocolServer.isLeaderlessPrimaryScmSigner(scm, nle, false));
  }
}
