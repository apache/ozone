/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Common test cases for {@link ShortLivedTokenVerifier} implementations.
 */
public abstract class TokenVerifierTests<T extends ShortLivedTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(TokenVerifierTests.class);

  protected static final String CERT_ID = "123";

  /**
   * Create the specific kind of TokenVerifier.
   */
  protected abstract TokenVerifier newTestSubject(
      SecurityConfig secConf, CertificateClient caClient);

  /**
   * @return the config key to enable/disable the specific kind of tokens
   */
  protected abstract String tokenEnabledConfigKey();

  /**
   * Create a request for which the verifier being tested does not require
   * tokens (eg. reading blocks does not require container token and vice versa)
   */
  protected abstract ContainerCommandRequestProto unverifiedRequest()
      throws IOException;

  /**
   * Create a request for which token should be required.
   */
  protected abstract ContainerCommandRequestProto verifiedRequest(T tokenId)
      throws IOException;

  protected abstract T newTokenId();

  @Test
  public void skipsVerificationIfDisabled() throws IOException {
    // GIVEN
    CertificateClient caClient = mock(CertificateClient.class);
    TokenVerifier subject = newTestSubject(tokenDisabled(), caClient);

    // WHEN
    subject.verify("anyUser", anyToken(), verifiedRequest(newTokenId()));

    // THEN
    verify(caClient, never()).getCertificate(any());
  }

  @Test
  public void skipsVerificationForMiscCommands() throws IOException {
    // GIVEN
    CertificateClient caClient = mock(CertificateClient.class);
    TokenVerifier subject = newTestSubject(tokenEnabled(), caClient);

    // WHEN
    subject.verify("anyUser", anyToken(), unverifiedRequest());

    // THEN
    verify(caClient, never()).getCertificate(any());
  }

  @Test
  public void rejectsExpiredCertificate() throws Exception {
    rejectsInvalidCertificate(CertificateExpiredException.class);
  }

  @Test
  public void rejectsNotYetValidCertificate() throws Exception {
    rejectsInvalidCertificate(CertificateNotYetValidException.class);
  }

  private void rejectsInvalidCertificate(
      Class<? extends CertificateException> problem) throws Exception {
    // GIVEN
    CertificateClient caClient = mock(CertificateClient.class);
    X509Certificate cert = invalidCertificate(problem);
    when(caClient.getCertificate(CERT_ID)).thenReturn(cert);
    ContainerCommandRequestProto cmd = verifiedRequest(newTokenId());
    TokenVerifier subject = newTestSubject(tokenEnabled(), caClient);

    // WHEN+THEN
    assertThrows(BlockTokenException.class, () ->
        subject.verify("anyUser", anyToken(), cmd));
  }

  @Test
  public void rejectsInvalidSignature() throws Exception {
    // GIVEN
    CertificateClient caClient = mock(CertificateClient.class);
    when(caClient.getCertificate(CERT_ID)).thenReturn(validCertificate());
    Token<?> invalidToken = new Token<>();
    validSignature(caClient, false);
    ContainerCommandRequestProto cmd = verifiedRequest(newTokenId());
    TokenVerifier subject = newTestSubject(tokenEnabled(), caClient);

    // WHEN+THEN
    assertThrows(BlockTokenException.class, () ->
        subject.verify("anyUser", invalidToken, cmd));
  }

  @Test
  public void rejectsExpiredToken() throws Exception {
    // GIVEN
    SecurityConfig conf = tokenEnabled();
    CertificateClient caClient = mock(CertificateClient.class);
    when(caClient.getCertificate(CERT_ID)).thenReturn(validCertificate());
    validSignature(caClient, true);
    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager(conf);
    T tokenId = expired(newTokenId());
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    Token<?> token = secretManager.generateToken(tokenId);
    TokenVerifier subject = newTestSubject(tokenEnabled(), caClient);

    // WHEN+THEN
    assertThrows(BlockTokenException.class, () ->
        subject.verify("anyUser", token, cmd));
  }

  @Test
  public void acceptsValidToken() throws Exception {
    // GIVEN
    SecurityConfig conf = tokenEnabled();
    CertificateClient caClient = mock(CertificateClient.class);
    when(caClient.getCertificate(CERT_ID)).thenReturn(validCertificate());
    validSignature(caClient, true);
    ShortLivedTokenSecretManager<T> secretManager = new MockTokenManager(conf);
    T tokenId = valid(newTokenId());
    ContainerCommandRequestProto cmd = verifiedRequest(tokenId);
    Token<?> token = secretManager.generateToken(tokenId);
    TokenVerifier subject = newTestSubject(conf, caClient);

    // WHEN+THEN
    subject.verify("anyUser", token, cmd);
  }

  private T expired(T tokenId) {
    tokenId.setExpiry(Instant.now().minusSeconds(3600));
    return tokenId;
  }

  private T valid(T tokenId) {
    tokenId.setExpiry(Instant.now().plusSeconds(3600));
    return tokenId;
  }

  private void validSignature(CertificateClient caClient, boolean valid)
      throws Exception {
    when(caClient.verifySignature(any(byte[].class), any(), any()))
        .thenReturn(valid);
  }

  private static X509Certificate invalidCertificate(
      Class<? extends CertificateException> problem)
      throws CertificateExpiredException, CertificateNotYetValidException {
    X509Certificate cert = mock(X509Certificate.class);
    doThrow(problem).when(cert).checkValidity();
    return cert;
  }

  private static X509Certificate validCertificate() {
    return mock(X509Certificate.class);
  }

  protected SecurityConfig tokenDisabled() {
    return getSecurityConfig(false);
  }

  protected SecurityConfig tokenEnabled() {
    return getSecurityConfig(true);
  }

  private SecurityConfig getSecurityConfig(boolean tokenEnabled) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(tokenEnabledConfigKey(), tokenEnabled);
    return new SecurityConfig(conf);
  }

  private static Token<?> anyToken() {
    return new Token<>();
  }

  /**
   * Mock secret manager for test.  No private key etc.
   */
  private class MockTokenManager extends ShortLivedTokenSecretManager<T> {

    MockTokenManager(SecurityConfig conf) {
      super(conf, TimeUnit.HOURS.toMillis(1), CERT_ID, LOG);
    }

    @Override
    public byte[] createPassword(T identifier) {
      return "asdf".getBytes(UTF_8);
    }
  }
}
