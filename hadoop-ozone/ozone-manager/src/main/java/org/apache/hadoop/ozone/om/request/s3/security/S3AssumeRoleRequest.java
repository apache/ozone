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

import static org.apache.hadoop.ozone.om.helpers.S3STSUtils.STS_ACCESS_KEY_ID_ALLOWED_CHARS;
import static org.apache.hadoop.ozone.om.helpers.S3STSUtils.STS_ACCESS_KEY_ID_ALLOWED_CHARS_LENGTH;
import static org.apache.hadoop.ozone.om.helpers.S3STSUtils.STS_ACCESS_KEY_ID_RANDOM_LENGTH;
import static org.apache.hadoop.ozone.om.helpers.S3STSUtils.STS_TOKEN_PREFIX;
import static org.apache.hadoop.ozone.security.acl.AssumeRoleRequest.OzoneGrant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.ipc_.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.AwsRoleArnValidator;
import org.apache.hadoop.ozone.om.helpers.S3STSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UpdateAssumeRoleRequest;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.iam.IamSessionPolicyResolver;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles S3AssumeRoleRequest request.
 */
public class S3AssumeRoleRequest extends OMClientRequest {

  private static final SecureRandom SECURE_RANDOM;

  static {
    SecureRandom secureRandom;
    try {
      // Prefer non-blocking native PRNG where available
      secureRandom = SecureRandom.getInstance("NativePRNGNonBlocking");
    } catch (Exception e) {
      // Fallback to default SecureRandom implementation
      secureRandom = new SecureRandom();
    }
    SECURE_RANDOM = secureRandom;
  }

  private static final int STS_SECRET_ACCESS_KEY_LENGTH = 40;
  private static final int STS_ROLE_ID_LENGTH = 16;
  private static final String ASSUME_ROLE_ID_PREFIX = "AROA";
  private static final String CHARS_FOR_SECRET_ACCESS_KEYS = STS_ACCESS_KEY_ID_ALLOWED_CHARS +
      "abcdefghijklmnopqrstuvwxyz/+";
  private static final int CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH = CHARS_FOR_SECRET_ACCESS_KEYS.length();

  private final Clock clock;

  public S3AssumeRoleRequest(OMRequest omRequest, Clock clock) {
    super(omRequest);
    this.clock = clock;
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    final OMRequest omRequest = super.preExecute(ozoneManager);
    final AssumeRoleRequest assumeRoleRequest = omRequest.getAssumeRoleRequest();

    final int durationSeconds = assumeRoleRequest.getDurationSeconds();
    final String roleSessionName = assumeRoleRequest.getRoleSessionName();
    final String roleArn = assumeRoleRequest.getRoleArn();
    final String awsIamSessionPolicy = assumeRoleRequest.getAwsIamSessionPolicy();
    final String requestId = assumeRoleRequest.getRequestId();
    final OzoneManagerProtocolProtos.UserInfo userInfo = omRequest.getUserInfo();
    final AuditLogger auditLogger = ozoneManager.getAuditLogger();
    final Map<String, String> auditMap = new HashMap<>();
    S3STSUtils.addAssumeRoleAuditParams(
        auditMap, roleArn, roleSessionName, awsIamSessionPolicy, durationSeconds, requestId);

    try {
      if (!omRequest.hasS3Authentication()) {
        throw new OMException(
            "S3AssumeRoleRequest does not have S3 authentication", OMException.ResultCodes.INVALID_REQUEST);
      }

      // Brief overview of flow:
      // The STS Endpoint makes the AssumeRole call, which when received by OM leader (via this method),
      // it will validate the request, authorize via Ranger, generate temporary credentials
      // (tempAccessKeyId, secretAccessKey), roleId, and the signed session token.
      // The original AssumeRole request is converted to an UpdateAssumeRoleRequest with the generated
      // values. This update request will be submitted to Ratis and replicated across all OMs.
      // All OMs in HA mode therefore will have identical audit logs with the same tempAccessKeyId.
      S3STSUtils.validateDuration(durationSeconds);
      S3STSUtils.validateRoleSessionName(roleSessionName);
      final String targetRoleName = AwsRoleArnValidator.validateAndExtractRoleNameFromArn(roleArn);
      
      // Generate temporary AWS credentials using cryptographically strong SecureRandom
      final String tempAccessKeyId = STS_TOKEN_PREFIX + generateSecureRandomStringUsingChars(
          STS_ACCESS_KEY_ID_ALLOWED_CHARS, STS_ACCESS_KEY_ID_ALLOWED_CHARS_LENGTH,
          STS_ACCESS_KEY_ID_RANDOM_LENGTH);
      final String secretAccessKey = generateSecureRandomStringUsingChars(
          CHARS_FOR_SECRET_ACCESS_KEYS, CHARS_FOR_SECRET_ACCESS_KEYS_LENGTH, STS_SECRET_ACCESS_KEY_LENGTH);
      final String roleId = ASSUME_ROLE_ID_PREFIX + generateSecureRandomStringUsingChars(
          STS_ACCESS_KEY_ID_ALLOWED_CHARS, STS_ACCESS_KEY_ID_ALLOWED_CHARS_LENGTH,
          STS_ROLE_ID_LENGTH);
      final String assumedRoleId = roleId + ":" + roleSessionName;
      final String assumedRoleUserArn = S3STSUtils.toAssumedRoleUserArn(roleArn, roleSessionName);

      final Instant creationInstant = clock.instant();
      final String sessionToken = generateSessionToken(GenerateSessionTokenParams.newBuilder()
          .setTargetRoleName(targetRoleName)
          .setOmRequest(omRequest)
          .setOzoneManager(ozoneManager)
          .setAssumeRoleRequest(assumeRoleRequest)
          .setSecretAccessKey(secretAccessKey)
          .setTempAccessKeyId(tempAccessKeyId)
          .setAssumedRoleId(assumedRoleId)
          .setAssumedRoleUserArn(assumedRoleUserArn)
          .setCreationTime(creationInstant)
          .build());
      final long expirationEpochSeconds = creationInstant.plusSeconds(durationSeconds).getEpochSecond();
      auditMap.put(OzoneConsts.S3_STS_TEMP_ACCESS_KEY_ID, tempAccessKeyId);

      // Build UpdateAssumeRoleRequest with leader-generated credentials and session token
      final UpdateAssumeRoleRequest.Builder updateAssumeRoleRequestBuilder =
          UpdateAssumeRoleRequest.newBuilder()
              .setRoleArn(roleArn)
              .setRoleSessionName(roleSessionName)
              .setDurationSeconds(durationSeconds)
              .setRequestId(requestId)
              .setTempAccessKeyId(tempAccessKeyId)
              .setSecretAccessKey(secretAccessKey)
              .setRoleId(roleId)
              .setSessionToken(sessionToken)
              .setExpirationEpochSeconds(expirationEpochSeconds);

      if (assumeRoleRequest.hasAwsIamSessionPolicy()) {
        updateAssumeRoleRequestBuilder.setAwsIamSessionPolicy(awsIamSessionPolicy);
      }

      return omRequest.toBuilder()
          .setUpdateAssumeRoleRequest(updateAssumeRoleRequestBuilder.build())
          .build();
    } catch (OMException e) {
      markForAudit(auditLogger, buildAuditMessage(OMAction.S3_ASSUME_ROLE, auditMap, e, userInfo));
      throw e;
    } catch (IOException e) {
      final OMException omException = new OMException(
          "Failed to generate STS token for role: " + roleArn, e, OMException.ResultCodes.INTERNAL_ERROR);
      markForAudit(auditLogger, buildAuditMessage(OMAction.S3_ASSUME_ROLE, auditMap, omException, userInfo));
      throw omException;
    }
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final OMRequest omRequest = getOmRequest();
    final AssumeRoleRequest assumeRoleRequest = omRequest.getAssumeRoleRequest();
    final UpdateAssumeRoleRequest updateAssumeRoleRequest = omRequest.getUpdateAssumeRoleRequest();

    final int durationSeconds = assumeRoleRequest.getDurationSeconds();
    final String roleSessionName = assumeRoleRequest.getRoleSessionName();
    final String roleArn = assumeRoleRequest.getRoleArn();
    final String awsIamSessionPolicy = assumeRoleRequest.getAwsIamSessionPolicy();
    final String requestId = assumeRoleRequest.getRequestId();

    // Extract leader-generated credentials and roleId from UpdateAssumeRoleRequest
    final String tempAccessKeyId = updateAssumeRoleRequest.getTempAccessKeyId();
    final String secretAccessKey = updateAssumeRoleRequest.getSecretAccessKey();
    final String roleId = updateAssumeRoleRequest.getRoleId();
    final String sessionToken = updateAssumeRoleRequest.getSessionToken();
    final long expirationEpochSeconds = updateAssumeRoleRequest.getExpirationEpochSeconds();

    final Map<String, String> auditMap = new HashMap<>();
    final AuditLogger auditLogger = ozoneManager.getAuditLogger();
    final OzoneManagerProtocolProtos.UserInfo userInfo = omRequest.getUserInfo();
    S3STSUtils.addAssumeRoleAuditParams(
        auditMap, roleArn, roleSessionName, awsIamSessionPolicy, durationSeconds, requestId);

    Exception exception = null;
    OMClientResponse omClientResponse;
    try {
      if (Strings.isNullOrEmpty(tempAccessKeyId) || Strings.isNullOrEmpty(secretAccessKey) ||
          Strings.isNullOrEmpty(roleId) || Strings.isNullOrEmpty(sessionToken) || expirationEpochSeconds <= 0) {
        throw new OMException(
            "UpdateAssumeRoleRequest is missing leader-generated AssumeRole fields",
            OMException.ResultCodes.INVALID_REQUEST);
      }

      final String assumedRoleId = roleId + ":" + roleSessionName;

      auditMap.put(OzoneConsts.S3_STS_TEMP_ACCESS_KEY_ID, tempAccessKeyId);

      final AssumeRoleResponse.Builder responseBuilder = AssumeRoleResponse.newBuilder()
          .setAccessKeyId(tempAccessKeyId)
          .setSecretAccessKey(secretAccessKey)
          .setSessionToken(sessionToken)
          .setExpirationEpochSeconds(expirationEpochSeconds)
          .setAssumedRoleId(assumedRoleId);

      omClientResponse = new S3AssumeRoleResponse(
          OmResponseUtil.getOMResponseBuilder(omRequest)
              .setAssumeRoleResponse(responseBuilder.build())
              .build());
    } catch (OMException e) {
      exception = e;
      omClientResponse = new S3AssumeRoleResponse(
          createErrorOMResponse(OmResponseUtil.getOMResponseBuilder(omRequest), e));
    }

    markForAudit(auditLogger, buildAuditMessage(OMAction.S3_ASSUME_ROLE, auditMap, exception, userInfo));

    return omClientResponse;
  }

  /**
   * Generates session token using components from the AssumeRoleRequest.
   */
  private String generateSessionToken(GenerateSessionTokenParams params) throws IOException {
    final OzoneManager ozoneManager = params.getOzoneManager();
    final OMRequest omRequest = params.getOmRequest();
    final AssumeRoleRequest assumeRoleRequest = params.getAssumeRoleRequest();

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    if (remoteIp == null) {
      remoteIp = ozoneManager.getOmRpcServerAddr().getAddress();
    }

    final String hostName = remoteIp != null ? remoteIp.getHostName() : ozoneManager.getOmRpcServerAddr().getHostName();

    // Determine the caller's access key ID - this will be referred to as the original
    // access key id.  When STS tokens are used, the tokens will be authorized as
    // the kerberos principal associated to the original access key id, in conjunction with the
    // role permissions and optional AWS IAM session policy permissions.
    final String originalAccessKeyId = omRequest.getS3Authentication().getAccessId();

    final String principal = OzoneAclUtils.accessIdToUserPrincipal(originalAccessKeyId);
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(principal);

    final String roleArn = assumeRoleRequest.getRoleArn();
    final String sessionPolicy = getSessionPolicy(
        ozoneManager, originalAccessKeyId, assumeRoleRequest.getAwsIamSessionPolicy(), hostName, remoteIp, ugi,
        params.getTargetRoleName());

    return ozoneManager.getSTSTokenSecretManager().createSTSTokenString(
        STSTokenSecretManager.CreateSTSTokenParams.newBuilder()
            .setTempAccessKeyId(params.getTempAccessKeyId())
            .setOriginalAccessKeyId(originalAccessKeyId)
            .setRoleArn(roleArn)
            .setDurationSeconds(assumeRoleRequest.getDurationSeconds())
            .setSecretAccessKey(params.getSecretAccessKey())
            .setSessionPolicy(sessionPolicy)
            .setAssumedRoleId(params.getAssumedRoleId())
            .setAssumedRoleUserArn(params.getAssumedRoleUserArn())
            .setCreationTime(params)
            .build());
  }

  /**
   * Parameters for {@link #generateSessionToken(GenerateSessionTokenParams)}.
   */
  private static final class GenerateSessionTokenParams {
    private final String targetRoleName;
    private final OMRequest omRequest;
    private final OzoneManager ozoneManager;
    private final AssumeRoleRequest assumeRoleRequest;
    private final String secretAccessKey;
    private final String tempAccessKeyId;
    private final String assumedRoleId;
    private final String assumedRoleUserArn;
    private final Instant creationInstant;

    private GenerateSessionTokenParams(Builder builder) {
      this.targetRoleName = builder.targetRoleName;
      this.omRequest = builder.omRequest;
      this.ozoneManager = builder.ozoneManager;
      this.assumeRoleRequest = builder.assumeRoleRequest;
      this.secretAccessKey = builder.secretAccessKey;
      this.tempAccessKeyId = builder.tempAccessKeyId;
      this.assumedRoleId = builder.assumedRoleId;
      this.assumedRoleUserArn = builder.assumedRoleUserArn;
      this.creationInstant = builder.creationInstant;
    }

    static Builder newBuilder() {
      return new Builder();
    }

    String getTargetRoleName() {
      return targetRoleName;
    }

    OMRequest getOmRequest() {
      return omRequest;
    }

    OzoneManager getOzoneManager() {
      return ozoneManager;
    }

    AssumeRoleRequest getAssumeRoleRequest() {
      return assumeRoleRequest;
    }

    String getSecretAccessKey() {
      return secretAccessKey;
    }

    String getTempAccessKeyId() {
      return tempAccessKeyId;
    }

    String getAssumedRoleId() {
      return assumedRoleId;
    }

    String getAssumedRoleUserArn() {
      return assumedRoleUserArn;
    }

    Instant getCreationTime() {
      return creationInstant;
    }

    private static final class Builder {
      private String targetRoleName;
      private OMRequest omRequest;
      private OzoneManager ozoneManager;
      private AssumeRoleRequest assumeRoleRequest;
      private String secretAccessKey;
      private String tempAccessKeyId;
      private String assumedRoleId;
      private String assumedRoleUserArn;
      private Instant creationInstant;

      Builder setTargetRoleName(String value) {
        this.targetRoleName = value;
        return this;
      }

      Builder setOmRequest(OMRequest value) {
        this.omRequest = value;
        return this;
      }

      Builder setOzoneManager(OzoneManager value) {
        this.ozoneManager = value;
        return this;
      }

      Builder setAssumeRoleRequest(AssumeRoleRequest value) {
        this.assumeRoleRequest = value;
        return this;
      }

      Builder setSecretAccessKey(String value) {
        this.secretAccessKey = value;
        return this;
      }

      Builder setTempAccessKeyId(String value) {
        this.tempAccessKeyId = value;
        return this;
      }

      Builder setAssumedRoleId(String value) {
        this.assumedRoleId = value;
        return this;
      }

      Builder setAssumedRoleUserArn(String value) {
        this.assumedRoleUserArn = value;
        return this;
      }

      public void setCreationTime(Instant creationInstant) {
        this.creationInstant = creationInstant;
      }

      GenerateSessionTokenParams build() {
        return new GenerateSessionTokenParams(this);
      }
    }
  }

  /**
   * Calls utility to convert IAM Policy to Ozone nomenclature and uses this output as input
   * to IAccessAuthorizer.generateAssumeRoleSessionPolicy() which is currently only implemented
   * by RangerOzoneAuthorizer.
   */
  @VisibleForTesting
  String getSessionPolicy(OzoneManager ozoneManager, String originalAccessKeyId, String awsIamPolicy,
      String hostName, InetAddress remoteIp, UserGroupInformation ugi, String targetRoleName) throws IOException {

    final String volumeName;
    if (ozoneManager.isS3MultiTenancyEnabled()) {
      final Optional<String> tenantOpt = ozoneManager.getMultiTenantManager()
          .getTenantForAccessID(originalAccessKeyId);
      if (tenantOpt.isPresent()) {
        volumeName = ozoneManager.getMultiTenantManager()
            .getTenantVolumeName(tenantOpt.get());
      } else {
        volumeName = HddsClientUtils.getDefaultS3VolumeName(ozoneManager.getConfiguration());
      }
    } else {
      volumeName = HddsClientUtils.getDefaultS3VolumeName(ozoneManager.getConfiguration());
    }

    final Set<OzoneGrant> grants = Strings.isNullOrEmpty(awsIamPolicy) ?
        null :
        resolveGrantsAgainstBucketLinks(
            IamSessionPolicyResolver.resolve(awsIamPolicy, volumeName, IamSessionPolicyResolver.AuthorizerType.RANGER),
            (linkVolume, linkBucket) -> ozoneManager.resolveBucketLink(Pair.of(linkVolume, linkBucket), true, false));

    return ozoneManager.getAccessAuthorizer().generateAssumeRoleSessionPolicy(
        new org.apache.hadoop.ozone.security.acl.AssumeRoleRequest(
            hostName, remoteIp, ugi, targetRoleName, grants));
  }

  /**
   * Rewrites the resolved session-policy grants so that any bucket, key, or prefix resource that names a
   * bucket link is anchored to the link's source volume and bucket - the resource paths the OM authorizes
   * against once the link is resolved at request time. READ on each link bucket in the chain (and, when the
   * chain crosses volumes, READ on each distinct volume except the requested one) is retained so OM can follow
   * every hop at request time, which keeps the generated token as small as possible.
   * <p>
   * The link target is resolved when the token is generated, so the token grants access to whatever the link
   * points to at that moment. If the link is later re-pointed, the token no longer grants access to the new
   * target.
   *
   * @param grants       the grants produced by {@link IamSessionPolicyResolver}, possibly {@code null}
   * @param linkResolver resolves a (volume, bucket) pair to its link target
   * @return the link-aware grants, or the input unchanged when there is nothing to resolve
   */
  @VisibleForTesting
  static Set<OzoneGrant> resolveGrantsAgainstBucketLinks(Set<OzoneGrant> grants,
      BucketLinkResolver linkResolver) throws IOException {
    if (grants == null || grants.isEmpty()) {
      return grants;
    }

    final Map<Pair<String, String>, ResolvedBucket> resolutionCache = new HashMap<>();
    final Set<IOzoneObj> linkFollowObjects = new LinkedHashSet<>();
    final Set<OzoneGrant> resolvedGrants = new LinkedHashSet<>();

    for (OzoneGrant grant : grants) {
      final Set<IOzoneObj> resolvedObjects = new LinkedHashSet<>();
      for (IOzoneObj object : grant.getObjects()) {
        resolvedObjects.add(
            resolveObjectAgainstBucketLink((OzoneObj) object, linkResolver, resolutionCache, linkFollowObjects));
      }
      resolvedGrants.add(new OzoneGrant(resolvedObjects, grant.getPermissions(), grant.getS3Actions()));
    }

    // Retain only the READ required to follow each link hop at request time.
    if (!linkFollowObjects.isEmpty()) {
      resolvedGrants.add(new OzoneGrant(linkFollowObjects, EnumSet.of(ACLType.READ)));
    }

    return resolvedGrants;
  }

  /**
   * Resolves a single grant object against its bucket link. Bucket, key, and prefix objects that name a
   * link bucket are rewritten to the link's source volume and bucket, and the READ needed to follow each hop
   * in the link chain is collected in {@code linkFollowObjects}. All other objects (volume resources and
   * wildcard buckets) are returned unchanged.
   */
  private static IOzoneObj resolveObjectAgainstBucketLink(OzoneObj object, BucketLinkResolver linkResolver,
      Map<Pair<String, String>, ResolvedBucket> resolutionCache, Set<IOzoneObj> linkFollowObjects)
      throws IOException {
    final OzoneObj.ResourceType resourceType = object.getResourceType();
    if (resourceType != OzoneObj.ResourceType.BUCKET
        && resourceType != OzoneObj.ResourceType.KEY
        && resourceType != OzoneObj.ResourceType.PREFIX) {
      return object;
    }

    final String volumeName = object.getVolumeName();
    final String bucketName = object.getBucketName();
    // Wildcard or unspecified names cannot correspond to a concrete link bucket.
    if (StringUtils.isBlank(volumeName) || StringUtils.isBlank(bucketName) || hasWildcard(volumeName) ||
        hasWildcard(bucketName)) {
      return object;
    }

    final Pair<String, String> requested = Pair.of(volumeName, bucketName);
    ResolvedBucket resolved = resolutionCache.get(requested);
    if (resolved == null) {
      resolved = linkResolver.resolve(volumeName, bucketName);
      resolutionCache.put(requested, resolved);
    }
    if (resolved == null || resolved.isDangling() || !resolved.isLink()) {
      return object;
    }

    final Set<String> chainVolumes = new LinkedHashSet<>();
    for (Pair<String, String> link : resolved.linkChain()) {
      linkFollowObjects.add(newResourceObj(OzoneObj.ResourceType.BUCKET, link.getLeft(), link.getRight()));
      chainVolumes.add(link.getLeft());
    }
    chainVolumes.add(resolved.realVolume());
    chainVolumes.remove(volumeName);
    for (String vol : chainVolumes) {
      linkFollowObjects.add(newResourceObj(OzoneObj.ResourceType.VOLUME, vol, null));
    }

    return OzoneObjInfo.Builder.fromOzoneObj(object)
        .setVolumeName(resolved.realVolume())
        .setBucketName(resolved.realBucket())
        .build();
  }

  private static IOzoneObj newResourceObj(OzoneObj.ResourceType resourceType, String volumeName, String bucketName) {
    final OzoneObjInfo.Builder builder = OzoneObjInfo.Builder.newBuilder()
        .setResType(resourceType)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName);
    if (bucketName != null) {
      builder.setBucketName(bucketName);
    }
    return builder.build();
  }

  private static boolean hasWildcard(String name) {
    return name.indexOf('*') >= 0 || name.indexOf('?') >= 0;
  }

  /**
   * Resolves a (volume, bucket) pair to its link target, following bucket links. Implementations must not
   * enforce ACLs, so that session-policy generation stays deterministic across OMs and does not depend on
   * the external authorizer.
   */
  @FunctionalInterface
  interface BucketLinkResolver {
    ResolvedBucket resolve(String volumeName, String bucketName) throws IOException;
  }

  /**
   * Generates a cryptographically strong String of the supplied stringLength using supplied chars.
   */
  @VisibleForTesting
  static String generateSecureRandomStringUsingChars(String chars, int charsLength, int stringLength) {
    final StringBuilder sb = new StringBuilder(stringLength);
    for (int i = 0; i < stringLength; i++) {
      sb.append(chars.charAt(SECURE_RANDOM.nextInt(charsLength)));
    }
    return sb.toString();
  }
}
