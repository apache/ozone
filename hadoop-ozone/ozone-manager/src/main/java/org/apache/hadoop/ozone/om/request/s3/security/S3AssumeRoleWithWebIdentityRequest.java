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

package org.apache.hadoop.ozone.om.request.s3.security;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_URI;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.ACCESS_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_TOKEN;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_EXPIRED;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.AwsRoleArnValidator;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3AssumeRoleWithWebIdentityResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleWithWebIdentityRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleWithWebIdentityResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateAssumeRoleWithWebIdentityRequest;
import org.apache.hadoop.ozone.security.oidc.AuthCredentials;
import org.apache.hadoop.ozone.security.oidc.CachingJwksProvider;
import org.apache.hadoop.ozone.security.oidc.OidcAuthenticationException;
import org.apache.hadoop.ozone.security.oidc.OidcConfig;
import org.apache.hadoop.ozone.security.oidc.OidcJwtIdentityProvider;
import org.apache.hadoop.ozone.security.oidc.OzoneIdentity;
import org.apache.hadoop.ozone.security.oidc.OzoneIdentityProvider;
import org.apache.hadoop.ozone.security.oidc.UrlJwksFetcher;

/**
 * Handles STS AssumeRoleWithWebIdentity requests.
 *
 * Raw WebIdentityToken is accepted only in the external OM RPC request. The
 * leader validates it in {@link #preExecute(OzoneManager)} and returns a
 * sanitized OMRequest for Ratis replication.
 */
public class S3AssumeRoleWithWebIdentityRequest extends OMClientRequest {

  private final Clock clock;
  private final OzoneIdentityProvider testIdentityProvider;

  public S3AssumeRoleWithWebIdentityRequest(OMRequest omRequest, Clock clock) {
    this(omRequest, clock, null);
  }

  @VisibleForTesting
  S3AssumeRoleWithWebIdentityRequest(OMRequest omRequest, Clock clock,
      OzoneIdentityProvider identityProvider) {
    super(omRequest);
    this.clock = clock;
    this.testIdentityProvider = identityProvider;
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final AssumeRoleWithWebIdentityRequest request =
        getOmRequest().getAssumeRoleWithWebIdentityRequest();
    final OidcConfig oidcConfig;
    try {
      oidcConfig = OidcConfig.from(ozoneManager.getConfiguration());
    } catch (IllegalArgumentException e) {
      throw new OMException("Invalid STS WebIdentity OIDC configuration",
          e, INVALID_REQUEST);
    }
    if (!oidcConfig.isEnabled()) {
      throw new OMException("STS WebIdentity is not enabled. Please set "
          + "ozone.sts.web.identity.enabled to true and restart all OMs.",
          FEATURE_NOT_ENABLED);
    }

    final int requestedDuration =
        S3STSUtils.validateDuration(request.getDurationSeconds());
    S3STSUtils.validateRoleSessionName(request.getRoleSessionName());
    AwsRoleArnValidator.validateAndExtractRoleNameFromArn(request.getRoleArn());

    final OzoneIdentity identity = authenticate(oidcConfig,
        request.getWebIdentityToken());
    final Instant issuedAt = clock.instant();
    final int effectiveDuration =
        clampDurationToTokenLifetime(requestedDuration, identity, issuedAt);
    final long credentialExpirationEpochSeconds =
        issuedAt.plusSeconds(effectiveDuration).getEpochSecond();
    final String tokenFingerprint =
        sha256Hex(request.getWebIdentityToken());

    final String sessionPolicy = generateSessionPolicy(ozoneManager,
        identity, request, oidcConfig.getAudience());
    if (Strings.isNullOrEmpty(sessionPolicy)) {
      throw new OMException("AssumeRoleWithWebIdentity was denied because "
          + "the authorizer returned no session policy", ACCESS_DENIED);
    }

    final UpdateAssumeRoleWithWebIdentityRequest updateRequest =
        UpdateAssumeRoleWithWebIdentityRequest.newBuilder()
            .setRoleArn(request.getRoleArn())
            .setRoleSessionName(request.getRoleSessionName())
            .setDurationSeconds(effectiveDuration)
            .setProviderId(request.hasProviderId() ? request.getProviderId() : "")
            .setRequestId(request.getRequestId())
            .setTempAccessKeyId(S3AssumeRoleRequest.generateTempAccessKeyId())
            .setSecretAccessKey(S3AssumeRoleRequest.generateSecretAccessKey())
            .setRoleId(S3AssumeRoleRequest.generateRoleId())
            .setEffectiveUser(identity.getUsername())
            .setSubject(identity.getSubject())
            .setIssuer(identity.getIssuer())
            .setAudience(oidcConfig.getAudience())
            .addAllGroups(identity.getGroups())
            .addAllRoles(identity.getRoles())
            .setWebIdentityTokenExpiresAt(identity.getExpiresAt().toEpochMilli())
            .setAuthenticatedAt(identity.getAuthenticatedAt().toEpochMilli())
            .setTokenFingerprint(tokenFingerprint)
            .setSessionPolicy(sessionPolicy)
            .setCredentialExpirationEpochSeconds(
                credentialExpirationEpochSeconds)
            .build();

    final OMRequest.Builder omRequest = OMRequest.newBuilder()
        .setUserInfo(getUserInfo().toBuilder()
            .setUserName(identity.getUsername()))
        .setCmdType(getOmRequest().getCmdType())
        .setClientId(getOmRequest().getClientId())
        .setUpdateAssumeRoleWithWebIdentityRequest(updateRequest);

    if (getOmRequest().hasTraceID()) {
      omRequest.setTraceID(getOmRequest().getTraceID());
    }

    return omRequest.build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      ExecutionContext context) {
    final OMRequest omRequest = getOmRequest();
    final UpdateAssumeRoleWithWebIdentityRequest updateRequest =
        omRequest.getUpdateAssumeRoleWithWebIdentityRequest();

    final Map<String, String> auditMap = new HashMap<>();
    final AuditLogger auditLogger = ozoneManager.getAuditLogger();
    addAuditParams(auditMap, updateRequest);

    Exception exception = null;
    OMClientResponse omClientResponse;
    try {
      S3STSUtils.validateDuration(updateRequest.getDurationSeconds());
      S3STSUtils.validateRoleSessionName(updateRequest.getRoleSessionName());
      AwsRoleArnValidator.validateAndExtractRoleNameFromArn(
          updateRequest.getRoleArn());
      if (Strings.isNullOrEmpty(updateRequest.getSessionPolicy())) {
        throw new OMException("Missing WebIdentity session policy",
            ACCESS_DENIED);
      }

      final String sessionToken = ozoneManager.getSTSTokenSecretManager()
          .createWebIdentitySTSTokenString(
              updateRequest.getTempAccessKeyId(),
              updateRequest.getRoleArn(),
              Instant.ofEpochSecond(
                  updateRequest.getCredentialExpirationEpochSeconds()),
              updateRequest.getSecretAccessKey(),
              updateRequest.getSessionPolicy(),
              updateRequest.getEffectiveUser(),
              updateRequest.getIssuer(),
              updateRequest.getSubject(),
              updateRequest.getAudience(),
              new LinkedHashSet<>(updateRequest.getGroupsList()),
              new LinkedHashSet<>(updateRequest.getRolesList()),
              updateRequest.getRoleSessionName(),
              updateRequest.getProviderId(),
              updateRequest.getTokenFingerprint());

      final String assumedRoleId =
          updateRequest.getRoleId() + ":" + updateRequest.getRoleSessionName();
      final long expirationEpochSeconds =
          updateRequest.getCredentialExpirationEpochSeconds();

      final AssumeRoleWithWebIdentityResponse.Builder responseBuilder =
          AssumeRoleWithWebIdentityResponse.newBuilder()
              .setAccessKeyId(updateRequest.getTempAccessKeyId())
              .setSecretAccessKey(updateRequest.getSecretAccessKey())
              .setSessionToken(sessionToken)
              .setExpirationEpochSeconds(expirationEpochSeconds)
              .setAssumedRoleId(assumedRoleId)
              .setSubjectFromWebIdentityToken(updateRequest.getSubject())
              .setAudience(updateRequest.getAudience());
      if (!Strings.isNullOrEmpty(updateRequest.getProviderId())) {
        responseBuilder.setProvider(updateRequest.getProviderId());
      }

      omClientResponse = new S3AssumeRoleWithWebIdentityResponse(
          OmResponseUtil.getOMResponseBuilder(omRequest)
              .setAssumeRoleWithWebIdentityResponse(responseBuilder.build())
              .build());
    } catch (OMException e) {
      exception = e;
      omClientResponse = new S3AssumeRoleWithWebIdentityResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest),
              e));
    } catch (IOException e) {
      final OMException omException = new OMException(
          "Failed to generate STS WebIdentity token", e, INTERNAL_ERROR);
      exception = omException;
      omClientResponse = new S3AssumeRoleWithWebIdentityResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest),
              omException));
    }

    markForAudit(auditLogger, buildAuditMessage(
        OMAction.S3_ASSUME_ROLE_WITH_WEB_IDENTITY, auditMap, exception,
        omRequest.getUserInfo()));
    return omClientResponse;
  }

  private OzoneIdentity authenticate(OidcConfig oidcConfig,
      String webIdentityToken) throws OMException {
    try {
      return identityProvider(oidcConfig)
          .authenticate(AuthCredentials.bearerToken(webIdentityToken));
    } catch (OidcAuthenticationException | IllegalArgumentException e) {
      throw new OMException("Invalid WebIdentityToken", e, INVALID_TOKEN);
    }
  }

  private OzoneIdentityProvider identityProvider(OidcConfig oidcConfig)
      throws OMException {
    if (testIdentityProvider != null) {
      return testIdentityProvider;
    }
    if (Strings.isNullOrEmpty(oidcConfig.getJwksUri())) {
      throw new OMException(OZONE_STS_WEB_IDENTITY_JWKS_URI
          + " must be configured when STS WebIdentity is enabled",
          INVALID_REQUEST);
    }
    try {
      URL jwksUrl = new URL(oidcConfig.getJwksUri());
      return new OidcJwtIdentityProvider(oidcConfig,
          new CachingJwksProvider(new UrlJwksFetcher(jwksUrl),
              oidcConfig.getJwksRefreshInterval()));
    } catch (MalformedURLException e) {
      throw new OMException(OZONE_STS_WEB_IDENTITY_JWKS_URI
          + " is not a valid URL", e, INVALID_REQUEST);
    }
  }

  private int clampDurationToTokenLifetime(int requestedDuration,
      OzoneIdentity identity, Instant now) throws OMException {
    long jwtRemainingSeconds =
        Duration.between(now, identity.getExpiresAt()).getSeconds();
    if (jwtRemainingSeconds < S3STSUtils.MIN_DURATION_SECONDS) {
      throw new OMException("WebIdentityToken expires before the minimum STS "
          + "credential duration can be issued", TOKEN_EXPIRED);
    }
    return (int) Math.min(requestedDuration, Math.min(jwtRemainingSeconds,
        S3STSUtils.MAX_DURATION_SECONDS));
  }

  private String generateSessionPolicy(OzoneManager ozoneManager,
      OzoneIdentity identity, AssumeRoleWithWebIdentityRequest request,
      String audience) throws OMException {
    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    if (remoteIp == null) {
      remoteIp = ozoneManager.getOmRpcServerAddr().getAddress();
    }
    final String hostName = remoteIp != null ? remoteIp.getHostName()
        : ozoneManager.getOmRpcServerAddr().getHostName();

    return ozoneManager.getAccessAuthorizer()
        .generateAssumeRoleWithWebIdentitySessionPolicy(
            new org.apache.hadoop.ozone.security.acl
                .AssumeRoleWithWebIdentityRequest(
                    hostName,
                    remoteIp,
                    identity.getUsername(),
                    identity.getGroups(),
                    identity.getRoles(),
                    request.getRoleArn(),
                    request.getRoleSessionName(),
                    identity.getIssuer(),
                    identity.getSubject(),
                    audience,
                    request.hasProviderId() ? request.getProviderId() : null,
                    null));
  }

  private static void addAuditParams(Map<String, String> auditMap,
      UpdateAssumeRoleWithWebIdentityRequest request) {
    auditMap.put("action", "AssumeRoleWithWebIdentity");
    auditMap.put("roleArn", request.getRoleArn());
    auditMap.put("roleSessionName", request.getRoleSessionName());
    auditMap.put("duration", String.valueOf(request.getDurationSeconds()));
    auditMap.put("providerId", request.getProviderId());
    auditMap.put("effectiveUser", request.getEffectiveUser());
    auditMap.put("issuer", request.getIssuer());
    auditMap.put("subject", request.getSubject());
    auditMap.put("audience", request.getAudience());
    auditMap.put("tokenFingerprint", request.getTokenFingerprint());
    auditMap.put("requestId", request.getRequestId());
  }

  private static String sha256Hex(String value) throws OMException {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256")
          .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      StringBuilder builder = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        builder.append(String.format("%02x", b));
      }
      return builder.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new OMException("SHA-256 digest is unavailable", e,
          INTERNAL_ERROR);
    }
  }
}
