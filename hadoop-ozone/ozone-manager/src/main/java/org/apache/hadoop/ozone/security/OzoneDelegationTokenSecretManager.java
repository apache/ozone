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

package org.apache.hadoop.ozone.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.OzoneSecretManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.security.OzoneSecretStore.OzoneManagerSecretState;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier.TokenInfo;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager for Ozone Master. Responsible for signing identifiers with
 * private key,
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneDelegationTokenSecretManager
    extends OzoneSecretManager<OzoneTokenIdentifier> {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneDelegationTokenSecretManager.class);
  private final Map<OzoneTokenIdentifier, TokenInfo> currentTokens;
  private final OzoneSecretStore store;
  private final S3SecretManager s3SecretManager;
  private Thread tokenRemoverThread;
  private final long tokenRemoverScanInterval;
  private final String omServiceId;
  private final OzoneManager ozoneManager;
  private SecretKeyClient secretKeyClient;

  /**
   * If the delegation token update thread holds this lock, it will not get
   * interrupted.
   */
  private final Object noInterruptsLock = new Object();

  /**
   * Create a secret manager with a builder object.
   *
   **/
  public OzoneDelegationTokenSecretManager(Builder b) throws IOException {
    super(new SecurityConfig(b.ozoneConf), b.tokenMaxLifetime,
        b.tokenRenewInterval, b.service, LOG);
    setCertClient(b.certClient);
    this.omServiceId = b.omServiceId;
    currentTokens = new ConcurrentHashMap<>();
    this.tokenRemoverScanInterval = b.tokenRemoverScanInterval;
    this.s3SecretManager = b.s3SecretManager;
    this.ozoneManager = b.ozoneManager;
    this.store = new OzoneSecretStore(b.ozoneConf,
        this.ozoneManager.getMetadataManager());
    this.secretKeyClient = b.secretKeyClient;
    loadTokenSecretState(store.loadState());
  }

  /**
   * Builder to help construct OzoneDelegationTokenSecretManager.
   */
  public static class Builder {
    private OzoneConfiguration ozoneConf;
    private long tokenMaxLifetime;
    private long tokenRenewInterval;
    private long tokenRemoverScanInterval;
    private Text service;
    private S3SecretManager s3SecretManager;
    private CertificateClient certClient;
    private String omServiceId;
    private OzoneManager ozoneManager;
    private SecretKeyClient secretKeyClient;

    public OzoneDelegationTokenSecretManager build() throws IOException {
      return new OzoneDelegationTokenSecretManager(this);
    }

    public Builder setConf(OzoneConfiguration conf) {
      this.ozoneConf = conf;
      return this;
    }

    public Builder setTokenMaxLifetime(long dtMaxLifetime) {
      this.tokenMaxLifetime = dtMaxLifetime;
      return this;
    }

    public Builder setTokenRenewInterval(long dtRenewInterval) {
      this.tokenRenewInterval = dtRenewInterval;
      return this;
    }

    public Builder setTokenRemoverScanInterval(long dtRemoverScanInterval) {
      this.tokenRemoverScanInterval = dtRemoverScanInterval;
      return this;
    }

    public Builder setService(Text dtService) {
      this.service = dtService;
      return this;
    }

    public Builder setS3SecretManager(S3SecretManager s3SecManager) {
      this.s3SecretManager = s3SecManager;
      return this;
    }

    public Builder setCertificateClient(CertificateClient certificateClient) {
      this.certClient = certificateClient;
      return this;
    }

    public Builder setSecretKeyClient(SecretKeyClient client) {
      this.secretKeyClient = client;
      return this;
    }

    public Builder setOmServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder setOzoneManager(OzoneManager ozoneMgr) {
      this.ozoneManager = ozoneMgr;
      return this;
    }
  }

  @Override
  public OzoneTokenIdentifier createIdentifier() {
    return OzoneTokenIdentifier.newInstance();
  }

  /**
   * Create new Identifier with given,owner,renwer and realUser.
   *
   * @return T
   */
  public OzoneTokenIdentifier createIdentifier(Text owner, Text renewer,
      Text realUser) {
    return OzoneTokenIdentifier.newInstance(owner, renewer, realUser);
  }

  /**
   * Returns {@link Token} for given identifier.
   *
   * @throws IOException to allow future exceptions to be added without breaking
   * compatibility
   */
  public Token<OzoneTokenIdentifier> createToken(Text owner, Text renewer,
      Text realUser)
      throws IOException {
    OzoneTokenIdentifier identifier = createIdentifier(owner, renewer,
        realUser);
    updateIdentifierDetails(identifier);
    byte[] password;
    if (ozoneManager.getVersionManager().isAllowed(OMLayoutFeature.DELEGATION_TOKEN_SYMMETRIC_SIGN)) {
      ManagedSecretKey currentSecretKey = secretKeyClient.getCurrentSecretKey();
      identifier.setSecretKeyId(currentSecretKey.getId().toString());
      password = currentSecretKey.sign(identifier.getBytes());
    } else {
      identifier.setOmCertSerialId(getCertSerialId());
      password = createPassword(identifier.getBytes(), getCurrentKey().getPrivateKey());
    }
    long expiryTime = identifier.getIssueDate() + getTokenRenewInterval();

    Token<OzoneTokenIdentifier> token = new Token<>(identifier.getBytes(),
        password, identifier.getKind(), getService());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created delegation token: {}", token);
    }
    return token;
  }

  /**
   * Add delegation token in to in-memory map of tokens.
   * @return renewTime - If updated successfully, return renewTime.
   */
  public long updateToken(Token<OzoneTokenIdentifier> token,
      OzoneTokenIdentifier ozoneTokenIdentifier, long tokenRenewInterval) {
    long renewTime = ozoneTokenIdentifier.getIssueDate() + tokenRenewInterval;
    TokenInfo tokenInfo = new TokenInfo(renewTime, token.getPassword(),
        ozoneTokenIdentifier.getTrackingId());
    currentTokens.put(ozoneTokenIdentifier, tokenInfo);
    return renewTime;
  }

  /**
   * Updates issue date, master key id and sequence number for identifier.
   *
   * @param identifier the identifier to validate
   */
  private void updateIdentifierDetails(OzoneTokenIdentifier identifier) {
    int sequenceNum;
    long now = Time.now();
    sequenceNum = incrementDelegationTokenSeqNum();
    identifier.setIssueDate(now);
    identifier.setMasterKeyId(getCurrentKey().getKeyId());
    identifier.setSequenceNumber(sequenceNum);
    identifier.setMaxDate(now + getTokenMaxLifetime());
    identifier.setOmServiceId(getOmServiceId());
  }

  private String getOmServiceId() {
    return omServiceId;
  }

  /**
   * Renew a delegation token.
   *
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  @Override
  public synchronized long renewToken(Token<OzoneTokenIdentifier> token,
      String renewer) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    OzoneTokenIdentifier id = OzoneTokenIdentifier.readProtoBuf(in);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Token renewal for identifier: {}, total currentTokens: {}",
          formatTokenId(id), currentTokens.size());
    }

    long now = Time.now();
    if (id.getMaxDate() < now) {
      throw new OMException(renewer + " tried to renew an expired token "
          + formatTokenId(id) + " max expiration date: "
          + Time.formatTime(id.getMaxDate())
          + " currentTime: " + Time.formatTime(now), TOKEN_EXPIRED);
    }
    validateToken(id);
    if ((id.getRenewer() == null) || (id.getRenewer().toString().isEmpty())) {
      throw new AccessControlException(renewer +
          " tried to renew a token " + formatTokenId(id)
          + " without a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new AccessControlException(renewer
          + " tries to renew a token " + formatTokenId(id)
          + " with non-matching renewer " + id.getRenewer());
    }

    return Math.min(id.getMaxDate(), now + getTokenRenewInterval());
  }

  public void updateRenewToken(Token<OzoneTokenIdentifier> token,
      OzoneTokenIdentifier ozoneTokenIdentifier, long expiryTime) {
    //TODO: Instead of having in-memory map inside this class, we can use
    // cache from table and make this table cache clean up policy NEVER. In
    // this way, we don't need to maintain separate in-memory map. To do this
    // work we need to merge HA/Non-HA code.
    TokenInfo tokenInfo = new TokenInfo(expiryTime, token.getPassword(),
        ozoneTokenIdentifier.getTrackingId());
    currentTokens.put(ozoneTokenIdentifier, tokenInfo);
  }

  /**
   * Cancel a token by removing it from store and cache.
   *
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  @Override
  public OzoneTokenIdentifier cancelToken(Token<OzoneTokenIdentifier> token,
      String canceller) throws IOException {
    OzoneTokenIdentifier id = OzoneTokenIdentifier.readProtoBuf(
        token.getIdentifier());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Token cancellation requested for identifier: {}",
          formatTokenId(id));
    }

    if (id.getUser() == null) {
      throw new InvalidToken("Token with no owner " + formatTokenId(id));
    }
    String owner = id.getUser().getUserName();
    Text renewer = id.getRenewer();
    HadoopKerberosName cancelerKrbName = new HadoopKerberosName(canceller);
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty()
        || !cancelerShortName
        .equals(renewer.toString()))) {
      throw new AccessControlException(canceller
          + " is not authorized to cancel the token " + formatTokenId(id));
    }

    // Check whether token is there in-memory map of tokens or not on the
    // OM leader.
    TokenInfo info = currentTokens.get(id);
    if (info == null) {
      throw new InvalidToken("Token not found in-memory map of tokens" +
          formatTokenId(id));
    }
    return id;
  }

  /**
   * Remove the expired token from in-memory map.
   */
  public void removeToken(OzoneTokenIdentifier ozoneTokenIdentifier) {
    currentTokens.remove(ozoneTokenIdentifier);
  }

  @Override
  public byte[] retrievePassword(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    // Tokens are a bit different in that a follower OM may be behind and
    // thus not yet know of all tokens issued by the leader OM.  the
    // following check does not allow ANY token auth. In optimistic, it should
    // allow known tokens in.
    try {
      ozoneManager.checkLeaderStatus();
    } catch (OMNotLeaderException | OMLeaderNotReadyException e) {
      InvalidToken wrappedStandby = new InvalidToken("IOException");
      wrappedStandby.initCause(e);
      throw wrappedStandby;
    }

    if (identifier.getTokenType().equals(S3AUTHINFO)) {
      return validateS3AuthInfo(identifier);
    }
    return validateToken(identifier).getPassword();
  }

  /**
   * Checks if TokenInfo for the given identifier exists in database and if the
   * token is expired.
   */
  private TokenInfo validateToken(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    TokenInfo info = currentTokens.get(identifier);
    if (info == null) {
      throw new InvalidToken("token " + formatTokenId(identifier)
          + " can't be found in cache");
    }
    long now = Time.now();
    if (info.getRenewDate() < now) {
      throw new InvalidToken("token " + formatTokenId(identifier) + " is " +
          "expired, current time: " + Time.formatTime(now) +
          " expected renewal time: " + Time.formatTime(info.getRenewDate()));
    }
    if (!verifySignature(identifier, info.getPassword())) {
      throw new InvalidToken("Tampered/Invalid token.");
    }
    return info;
  }

  /**
   * Validates if given hash is valid.
   * HDDS-8829 changes the delegation token from sign by OM's RSA private key to secret key supported by SCM.
   * The default delegation token lifetime is 7 days.
   * In the 7 days period after OM is upgraded from version without HDDS-8829 to version with HDDS-8829, tokens
   * signed by RSA private key, and tokens signed by secret key will coexist. After 7 days, there will be only
   * tokens signed by secrete key still valid. Following logic will handle both types of tokens.
   */
  public boolean verifySignature(OzoneTokenIdentifier identifier,
      byte[] password) {
    String secretKeyId = identifier.getSecretKeyId();
    if (StringUtils.isNotEmpty(secretKeyId)) {
      try {
        ManagedSecretKey verifyKey = secretKeyClient.getSecretKey(UUID.fromString(secretKeyId));
        if (verifyKey == null) {
          throw new SCMSecurityException("Secret verify key " + UUID.fromString(secretKeyId) +
              " not found for token " + formatTokenId(identifier));
        }
        return verifyKey.isValidSignature(identifier.getBytes(), password);
      } catch (SCMSecurityException e) {
        LOG.error("verifySignature for identifier {} failed", identifier, e);
        return false;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Verify an asymmetric key signed Token {}", identifier);
    }

    X509Certificate signerCert;
    try {
      signerCert = getCertClient().getCertificate(
          identifier.getOmCertSerialId());
    } catch (CertificateException e) {
      LOG.error("getCertificate failed for serialId {}", identifier.getOmCertSerialId(), e);
      return false;
    }

    if (signerCert == null) {
      LOG.error("signerCert is null for serialId {}", identifier.getOmCertSerialId());
      return false;
    }

    // Check for expired certificate or not yet valid certificate
    try {
      signerCert.checkValidity();
    } catch (CertificateExpiredException | CertificateNotYetValidException e) {
      LOG.error("signerCert {} is invalid", signerCert, e);
      return false;
    }

    try {
      return getCertClient().verifySignature(identifier.getBytes(), password,
          signerCert);
    } catch (CertificateException e) {
      LOG.error("verifySignature with signerCert {} failed", signerCert, e);
      return false;
    }
  }

  /**
   * Validates if a S3 identifier is valid or not.
   * */
  private byte[] validateS3AuthInfo(OzoneTokenIdentifier identifier)
      throws InvalidToken {
    LOG.trace("Validating S3AuthInfo for identifier:{}", identifier);
    if (identifier.getOwner() == null) {
      throw new InvalidToken(
          "Owner is missing from the S3 auth token");
    }
    if (!identifier.getOwner().toString().equals(identifier.getAwsAccessId())) {
      LOG.error(
          "Owner and AWSAccessId is different in the S3 token. Possible "
              + " security attack: {}",
          identifier);
      throw new InvalidToken(
          "Invalid S3 identifier: owner=" + identifier.getOwner()
              + ", awsAccessId=" + identifier.getAwsAccessId());
    }
    String awsSecret;
    try {
      awsSecret = s3SecretManager.getSecretString(identifier
          .getAwsAccessId());
    } catch (IOException e) {
      LOG.warn("S3 identifier validation failed:{}",
          identifier, e);
      throw new InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (awsSecret == null) {
      throw new InvalidToken("No S3 secret found for S3 identifier:"
          + identifier);
    }

    if (AWSV4AuthValidator.validateRequest(identifier.getStrToSign(),
        identifier.getSignature(), awsSecret)) {
      return identifier.getSignature().getBytes(UTF_8);
    }
    throw new InvalidToken("Invalid S3 identifier:"
        + identifier);

  }

  /**
   * Load delegation tokens from DB into memory.
   * HDDS-8829 changes the delegation token from sign by OM's RSA private key to secret key supported by SCM.
   * The default delegation token lifetime is 7 days. After OM is upgraded from version without HDDS-8829 to
   * version with HDDS-8829 and restarts, tokens signed by RSA private key will be loaded from DB into memory.
   * Next OM restarts, if after 7 days, there will be only tokens signed by secret key loaded into memory.
   * Both types of token loading should be supported.
   */
  private void loadTokenSecretState(
      OzoneManagerSecretState<OzoneTokenIdentifier> state) throws IOException {
    LOG.info("Loading token state into token manager.");
    for (Map.Entry<OzoneTokenIdentifier, Long> entry :
        state.getTokenState().entrySet()) {
      try {
        addPersistedDelegationToken(entry.getKey(), entry.getValue());
      } catch (Exception e) {
        LOG.error("exception while loading delegation token from DB... ignored to continue startup", e);
      }
    }
  }

  private void addPersistedDelegationToken(OzoneTokenIdentifier identifier,
      long renewDate) throws IOException {
    if (isRunning()) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }

    byte[] password;
    if (StringUtils.isNotEmpty(identifier.getSecretKeyId())) {
      ManagedSecretKey signKey = secretKeyClient.getSecretKey(UUID.fromString(identifier.getSecretKeyId()));
      if (signKey == null) {
        // if delegation token expired, remove it from the store.
        if (renewDate < Time.now()) {
          LOG.info("Removing expired persisted delegation token {} from DB", identifier);
          this.store.removeToken(identifier);
        }

        throw new IOException("Secret key " + UUID.fromString(identifier.getSecretKeyId()) +
            " not found for token " + formatTokenId(identifier));
      }
      password = signKey.sign(identifier.getBytes());
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Load an asymmetric key signed Token {}", identifier);
      }
      password = createPassword(identifier.getBytes(), getCertClient().getPrivateKey());
    }

    if (identifier.getSequenceNumber() > getDelegationTokenSeqNum()) {
      setDelegationTokenSeqNum(identifier.getSequenceNumber());
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new TokenInfo(renewDate,
          password, identifier.getTrackingId()));
    } else {
      throw new IOException("Same delegation token being added twice: "
          + formatTokenId(identifier));
    }
  }

  /**
   * Should be called before this object is used.
   */
  @Override
  public synchronized void start(CertificateClient certClient)
      throws IOException {
    super.start(certClient);
    tokenRemoverThread = new Daemon(new ExpiredTokenRemover());
    tokenRemoverThread.setName(
        ozoneManager.getThreadNamePrefix() +
            "ExpiredTokenRemover");
    tokenRemoverThread.start();
  }

  private synchronized void stopThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping expired delegation token remover thread");
    }
    setIsRunning(false);

    if (tokenRemoverThread != null) {
      synchronized (noInterruptsLock) {
        tokenRemoverThread.interrupt();
      }
      try {
        tokenRemoverThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Unable to join on token removal thread", e);
      }
    }
  }

  /**
   * Stops the OzoneDelegationTokenSecretManager.
   */
  @Override
  public void stop() throws IOException {
    super.stop();
    stopThreads();
  }

  @VisibleForTesting
  public void setSecretKeyClient(SecretKeyClient client) {
    this.secretKeyClient = client;
  }

  /**
   * Remove expired delegation tokens from cache and persisted store.
   */
  private void removeExpiredToken() {
    long now = Time.now();
    synchronized (noInterruptsLock) {
      Iterator<Map.Entry<OzoneTokenIdentifier,
          TokenInfo>> i = currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<OzoneTokenIdentifier,
            TokenInfo> entry = i.next();
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          i.remove();
          try {
            store.removeToken(entry.getKey());
          } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to remove expired token {}", entry.getValue());
            }
          }
        }
      }
    }
  }

  private class ExpiredTokenRemover extends Thread {

    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval={} min(s)",
              getTokenRemoverScanInterval() / (60 * 1000));
      try {
        while (isRunning()) {
          long now = Time.now();
          if (lastTokenCacheCleanup + getTokenRemoverScanInterval()
              < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }

          // Sleep for 5 seconds
          Thread.sleep(Math.min(5000, getTokenRemoverScanInterval()));

        }
      } catch (InterruptedException ie) {
        LOG.info("ExpiredTokenRemover was interrupted.", ie);
        Thread.currentThread().interrupt();
      } catch (Exception t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception",
            t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  public long getTokenRemoverScanInterval() {
    return tokenRemoverScanInterval;
  }
}
