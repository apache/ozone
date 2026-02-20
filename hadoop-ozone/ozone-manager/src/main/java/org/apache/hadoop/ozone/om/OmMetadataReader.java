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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.server.ServerUtils.getRemoteUserName;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_LISTING_PAGE_SIZE_MAX;
import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.Auditor;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.ListKeysLightResult;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.protocolPB.grpc.GrpcClientConstants;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

/**
 * OM Metadata Reading class for the OM and Snapshot managers.
 *
 * This abstraction manages all the metadata key/acl reading
 * from a rocksDb instance, for both the OM and OM snapshots.
 */
public class OmMetadataReader implements IOmMetadataReader, Auditor {
  private final KeyManager keyManager;
  private final PrefixManager prefixManager;
  private final VolumeManager volumeManager;
  private final BucketManager bucketManager;
  private final OzoneManager ozoneManager;
  private final boolean isAclEnabled;
  private final IAccessAuthorizer accessAuthorizer;
  private final OmMetadataReaderMetrics metrics;
  private final Logger log;
  private final AuditLogger audit;
  private final OMPerformanceMetrics perfMetrics;

  public OmMetadataReader(KeyManager keyManager,
                          PrefixManager prefixManager,
                          OzoneManager ozoneManager,
                          Logger log,
                          AuditLogger audit,
                          OmMetadataReaderMetrics omMetadataReaderMetrics,
                          IAccessAuthorizer accessAuthorizer) {
    this.keyManager = keyManager;
    this.bucketManager = ozoneManager.getBucketManager();
    this.volumeManager = ozoneManager.getVolumeManager();
    this.prefixManager = prefixManager;
    this.ozoneManager = ozoneManager;
    this.isAclEnabled = ozoneManager.getAclsEnabled();
    this.log = log;
    this.audit = audit;
    this.metrics = omMetadataReaderMetrics;
    this.perfMetrics = ozoneManager.getPerfMetrics();
    this.accessAuthorizer = accessAuthorizer != null ? accessAuthorizer
        : OzoneAccessAuthorizer.get();
  }

  /**
   * Lookup a key.
   *
   * @param args - attributes of the key.
   * @return OmKeyInfo - the info about the requested key.
   */
  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    long start = Time.monotonicNowNanos();
    ResolvedBucket bucket = captureLatencyNs(
        perfMetrics.getLookupResolveBucketLatencyNs(),
        () -> ozoneManager.resolveBucketLink(args));
    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    OmKeyArgs resolvedArgs = bucket.update(args);

    try {
      if (isAclEnabled) {
        captureLatencyNs(perfMetrics.getLookupAclCheckLatencyNs(),
            () -> checkAcls(ResourceType.KEY, StoreType.OZONE,
                ACLType.READ, bucket,
                args.getKeyName())
        );
      }
      metrics.incNumKeyLookups();
      return keyManager.lookupKey(resolvedArgs, bucket, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumKeyLookupFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
            auditMap));
      }

      perfMetrics.addLookupLatency(Time.monotonicNowNanos() - start);
    }
  }

  @Override
  public KeyInfoWithVolumeContext getKeyInfo(final OmKeyArgs args,
                                             boolean assumeS3Context)
      throws IOException {
    long start = Time.monotonicNowNanos();

    java.util.Optional<S3VolumeContext> s3VolumeContext =
        java.util.Optional.empty();

    final OmKeyArgs resolvedVolumeArgs;
    if (assumeS3Context) {
      S3VolumeContext context = ozoneManager.getS3VolumeContext(true);
      s3VolumeContext = java.util.Optional.of(context);
      resolvedVolumeArgs = args.toBuilder()
          .setVolumeName(context.getOmVolumeArgs().getVolume())
          .build();
    } else {
      resolvedVolumeArgs = args;
    }

    final ResolvedBucket bucket = captureLatencyNs(
        perfMetrics.getGetKeyInfoResolveBucketLatencyNs(),
        () -> ozoneManager.resolveBucketLink(resolvedVolumeArgs));

    boolean auditSuccess = true;
    OmKeyArgs resolvedArgs = bucket.update(resolvedVolumeArgs);

    try {
      if (isAclEnabled) {
        captureLatencyNs(perfMetrics.getGetKeyInfoAclCheckLatencyNs(), () ->
            checkAcls(ResourceType.KEY,
                StoreType.OZONE, ACLType.READ,
                bucket, args.getKeyName())
        );
      }

      metrics.incNumGetKeyInfo();
      OmKeyInfo keyInfo = keyManager.getKeyInfo(resolvedArgs, bucket,
              OmMetadataReader.getClientAddress());
      KeyInfoWithVolumeContext.Builder builder = KeyInfoWithVolumeContext
          .newBuilder()
          .setKeyInfo(keyInfo);
      s3VolumeContext.ifPresent(context -> {
        builder.setVolumeArgs(context.getOmVolumeArgs());
        builder.setUserPrincipal(context.getUserPrincipal());
      });
      return builder.build();
    } catch (Exception ex) {
      metrics.incNumGetKeyInfoFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.READ_KEY,
          bucket.audit(resolvedVolumeArgs.toAuditMap()), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.READ_KEY,
            bucket.audit(resolvedVolumeArgs.toAuditMap())));
      }
      perfMetrics.addGetKeyInfoLatencyNs(Time.monotonicNowNanos() - start);
    }
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {

    long maxListingPageSize = ozoneManager.getConfiguration().getInt(
        OZONE_FS_LISTING_PAGE_SIZE_MAX,
        OZONE_FS_LISTING_PAGE_SIZE_DEFAULT);
    maxListingPageSize = OzoneConfigUtil.limitValue(numEntries,
        OZONE_FS_LISTING_PAGE_SIZE, OZONE_FS_LISTING_PAGE_SIZE_MAX,
        maxListingPageSize);

    ResolvedBucket bucket = ozoneManager.resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      if (isAclEnabled) {
        checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
            bucket, args.getKeyName());
      }
      metrics.incNumListStatus();
      return keyManager.listStatus(args, recursive, startKey,
          maxListingPageSize, getClientAddress(), allowPartialPrefixes);
    } catch (Exception ex) {
      metrics.incNumListStatusFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_STATUS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LIST_STATUS, auditMap));
      }
    }
  }

  @Override
  public List<OzoneFileStatusLight> listStatusLight(OmKeyArgs args,
      boolean recursive, String startKey, long numEntries,
      boolean allowPartialPrefixes) throws IOException {
    List<OzoneFileStatus> ozoneFileStatuses =
        listStatus(args, recursive, startKey, numEntries, allowPartialPrefixes);

    return ozoneFileStatuses.stream()
        .map(OzoneFileStatusLight::fromOzoneFileStatus)
        .collect(Collectors.toList());
  }
  
  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = ozoneManager.resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      if (isAclEnabled) {
        checkAcls(getResourceType(args), StoreType.OZONE, ACLType.READ,
            bucket, args.getKeyName());
      }
      metrics.incNumGetFileStatus();
      return keyManager.getFileStatus(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumGetFileStatusFails();
      auditSuccess = false;
      audit.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_FILE_STATUS, auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_FILE_STATUS, auditMap));
      }
    }
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args) throws IOException {
    ResolvedBucket bucket = ozoneManager.resolveBucketLink(args);

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    args = bucket.update(args);

    try {
      if (isAclEnabled) {
        checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.READ,
            bucket, args.getKeyName());
      }
      metrics.incNumLookupFile();
      return keyManager.lookupFile(args, getClientAddress());
    } catch (Exception ex) {
      metrics.incNumLookupFileFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LOOKUP_FILE,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(
            OMAction.LOOKUP_FILE, auditMap));
      }
    }
  }

  @Override
  public ListKeysResult listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    long startNanos = Time.monotonicNowNanos();
    ResolvedBucket bucket = captureLatencyNs(
        perfMetrics.getListKeysResolveBucketLatencyNs(),
        () -> ozoneManager.resolveBucketLink(
            Pair.of(volumeName, bucketName)));

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit();
    auditMap.put(OzoneConsts.START_KEY, startKey);
    auditMap.put(OzoneConsts.MAX_KEYS, String.valueOf(maxKeys));
    auditMap.put(OzoneConsts.KEY_PREFIX, keyPrefix);

    try {
      if (isAclEnabled) {
        captureLatencyNs(perfMetrics.getListKeysAclCheckLatencyNs(), () ->
            checkAcls(ResourceType.BUCKET, StoreType.OZONE, ACLType.LIST,
            bucket.realVolume(), bucket.realBucket(), keyPrefix)
        );
      }
      metrics.incNumKeyLists();
      return keyManager.listKeys(bucket.realVolume(), bucket.realBucket(),
          startKey, keyPrefix, maxKeys);
    } catch (IOException ex) {
      metrics.incNumKeyListFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.LIST_KEYS,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.LIST_KEYS,
            auditMap));
      }
      perfMetrics.addListKeysLatencyNs(Time.monotonicNowNanos() - startNanos);
    }
  }

  @Override
  public ListKeysLightResult listKeysLight(String volumeName,
                                            String bucketName,
                                            String startKey, String keyPrefix,
                                            int maxKeys) throws IOException {
    ListKeysResult listKeysResult =
        listKeys(volumeName, bucketName, startKey, keyPrefix, maxKeys);
    List<OmKeyInfo> keys = listKeysResult.getKeys();
    List<BasicOmKeyInfo> basicKeysList =
        keys.stream().map(BasicOmKeyInfo::fromOmKeyInfo)
            .collect(Collectors.toList());

    return new ListKeysLightResult(basicKeysList, listKeysResult.isTruncated());
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {

    String volumeName = obj.getVolumeName();
    String bucketName = obj.getBucketName();
    String keyName = obj.getKeyName();
    if (obj.getResourceType() == ResourceType.KEY || obj.getResourceType() == ResourceType.PREFIX) {
      ResolvedBucket resolvedBucket = ozoneManager.resolveBucketLink(
          Pair.of(volumeName, bucketName));
      volumeName = resolvedBucket.realVolume();
      bucketName = resolvedBucket.realBucket();
    }
    boolean auditSuccess = true;

    try {
      if (isAclEnabled) {
        checkAcls(obj.getResourceType(), obj.getStoreType(), ACLType.READ_ACL,
            volumeName, bucketName, keyName);
      }
      metrics.incNumGetAcl();
      switch (obj.getResourceType()) {
      case VOLUME:
        return volumeManager.getAcl(obj);
      case BUCKET:
        return bucketManager.getAcl(obj);
      case KEY:
        return keyManager.getAcl(obj);
      case PREFIX:
        return prefixManager.getAcl(obj);

      default:
        throw new OMException("Unexpected resource type: " +
            obj.getResourceType(), INVALID_REQUEST);
      }
    } catch (Exception ex) {
      auditSuccess = false;
      audit.logReadFailure(
          buildAuditMessageForFailure(OMAction.GET_ACL, obj.toAuditMap(), ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(
            buildAuditMessageForSuccess(OMAction.GET_ACL, obj.toAuditMap()));
      }
    }
  }

  @Override
  public Map<String, String> getObjectTagging(OmKeyArgs args) throws IOException {
    long start = Time.monotonicNowNanos();

    ResolvedBucket bucket = captureLatencyNs(
        perfMetrics.getLookupResolveBucketLatencyNs(),
        () -> ozoneManager.resolveBucketLink(args));

    boolean auditSuccess = true;
    Map<String, String> auditMap = bucket.audit(args.toAuditMap());

    OmKeyArgs resolvedArgs = bucket.update(args);

    try {
      if (isAclEnabled) {
        captureLatencyNs(perfMetrics.getGetObjectTaggingAclCheckLatencyNs(),
            () -> checkAcls(ResourceType.KEY, StoreType.OZONE,
                ACLType.READ, bucket,
                args.getKeyName())
        );
      }
      metrics.incNumGetObjectTagging();
      return keyManager.getObjectTagging(resolvedArgs, bucket);
    } catch (Exception ex) {
      metrics.incNumGetObjectTaggingFails();
      auditSuccess = false;
      audit.logReadFailure(buildAuditMessageForFailure(OMAction.GET_OBJECT_TAGGING,
          auditMap, ex));
      throw ex;
    } finally {
      if (auditSuccess) {
        audit.logReadSuccess(buildAuditMessageForSuccess(OMAction.GET_OBJECT_TAGGING,
            auditMap));
      }

      perfMetrics.addGetObjectTaggingLatencyNs(Time.monotonicNowNanos() - start);
    }
  }

  /**
   * Checks if current caller has acl permissions.
   *
   * @param resType - Type of ozone resource. Ex volume, bucket.
   * @param store   - Store type. i.e Ozone, S3.
   * @param acl     - type of access to be checked.
   * @param vol     - name of volume
   * @param bucket  - bucket name
   * @param key     - key
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied.
   */
  void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, String vol, String bucket, String key)
      throws IOException {
    UserGroupInformation user;
    if (getS3Auth() != null) {
      final String effectiveAccessId = OzoneManager.getS3AuthEffectiveAccessId();
      String principal =
          OzoneAclUtils.accessIdToUserPrincipal(effectiveAccessId);
      user = UserGroupInformation.createRemoteUser(principal);
    } else {
      user = ProtobufRpcEngine.Server.getRemoteUser();
    }

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    String volumeOwner = ozoneManager.getVolumeOwner(vol, acl, resType);
    String bucketOwner = ozoneManager.getBucketOwner(vol, bucket, acl, resType);

    OzoneAclUtils.checkAllAcls(this, resType, store, acl,
        vol, bucket, key, volumeOwner, bucketOwner,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp :
            ozoneManager.getOmRpcServerAddr().getAddress(),
        remoteIp != null ? remoteIp.getHostName() :
            ozoneManager.getOmRpcServerAddr().getHostName());
  }

  /**
   * Checks if current caller has acl permissions.
   *
   * @param resType - Type of ozone resource. Ex volume, bucket.
   * @param store   - Store type. i.e Ozone, S3.
   * @param acl     - type of access to be checked.
   * @param resolvedBucket - resolved bucket information.
   * @param key     - key
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied.
   */
  void checkAcls(ResourceType resType, StoreType store,
      ACLType acl, ResolvedBucket resolvedBucket, String key)
      throws IOException {
    UserGroupInformation user;
    if (getS3Auth() != null) {
      final String effectiveAccessId = OzoneManager.getS3AuthEffectiveAccessId();
      String principal =
          OzoneAclUtils.accessIdToUserPrincipal(effectiveAccessId);
      user = UserGroupInformation.createRemoteUser(principal);
    } else {
      user = ProtobufRpcEngine.Server.getRemoteUser();
    }

    String vol = resolvedBucket.realVolume();
    String bucket = resolvedBucket.realBucket();
    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();
    String volumeOwner = ozoneManager.getVolumeOwner(vol, acl, resType);
    String bucketOwner = resolvedBucket.bucketOwner();

    OzoneAclUtils.checkAllAcls(this, resType, store, acl,
        vol, bucket, key, volumeOwner, bucketOwner,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp :
            ozoneManager.getOmRpcServerAddr().getAddress(),
        remoteIp != null ? remoteIp.getHostName() :
            ozoneManager.getOmRpcServerAddr().getHostName());
  }

  
  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  @SuppressWarnings("parameternumber")
  public boolean checkAcls(ResourceType resType, StoreType storeType,
      ACLType aclType, String vol, String bucket, String key,
      UserGroupInformation ugi, InetAddress remoteAddress, String hostName,
      boolean throwIfPermissionDenied, String owner)
      throws OMException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(storeType)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(ACLIdentityType.USER)
        .setAclRights(aclType)
        .setOwnerName(owner)
        .build();

    return checkAcls(obj, context, throwIfPermissionDenied);
  }

  /**
   * CheckAcls for the ozone object.
   *
   * @return true if permission granted, false if permission denied.
   * @throws OMException ResultCodes.PERMISSION_DENIED if permission denied
   *                     and throwOnPermissionDenied set to true.
   */
  public boolean checkAcls(OzoneObj obj, RequestContext context,
      boolean throwIfPermissionDenied) throws OMException {

    final RequestContext normalizedRequestContext = maybeAttachSessionPolicyFromThreadLocal(context);

    if (!captureLatencyNs(perfMetrics::setCheckAccessLatencyNs,
        () -> accessAuthorizer.checkAccess(obj, normalizedRequestContext))) {
      if (throwIfPermissionDenied) {
        String volumeName = obj.getVolumeName() != null ?
                "Volume:" + obj.getVolumeName() + " " : "";
        String bucketName = obj.getBucketName() != null ?
                "Bucket:" + obj.getBucketName() + " " : "";
        String keyName = obj.getKeyName() != null ?
                "Key:" + obj.getKeyName() : "";
        // For STS tokens, make clear that the user is using an assumed role, otherwise the access denied
        // message could be confusing
        String user = normalizedRequestContext.getClientUgi().getShortUserName();
        final STSTokenIdentifier stsTokenIdentifier = OzoneManager.getStsTokenIdentifier();
        if (stsTokenIdentifier != null) {
          final StringBuilder builder = new StringBuilder(user);
          builder.append(" (STS assumed role arn = ");
          builder.append(stsTokenIdentifier.getRoleArn());
          builder.append(", tempAccessKeyId = ");
          builder.append(stsTokenIdentifier.getTempAccessKeyId());
          builder.append(')');
          user = builder.toString();
        }
        log.warn("User {} doesn't have {} permission to access {} {}{}{}",
            user,
            normalizedRequestContext.getAclRights(),
            obj.getResourceType(), volumeName, bucketName, keyName);
        throw new OMException(
            "User " + user +
            " doesn't have " + normalizedRequestContext.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            volumeName  + bucketName + keyName, ResultCodes.PERMISSION_DENIED);
      }
      return false;
    } else {
      return true;
    }
  }

  /**
   * Attaches session policy to RequestContext if an STSTokenIdentifier is found in the Ozone Manager thread local
   * (meaning this is an STS request), and the STSTokenIdentifier has a session policy.  Otherwise, returns the
   * RequestContext as it was before.
   * @param context the original RequestContext
   * @return RequestContext as before or with sessionPolicy embedded
   */
  private RequestContext maybeAttachSessionPolicyFromThreadLocal(RequestContext context) {
    final STSTokenIdentifier stsTokenIdentifier = OzoneManager.getStsTokenIdentifier();
    if (stsTokenIdentifier == null) {
      return context;
    }

    return context.toBuilder()
        .setSessionPolicy(stsTokenIdentifier.getSessionPolicy())
        .build();
  }

  static String getClientAddress() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) { //not a RPC client
      String clientIpAddress =
          GrpcClientConstants.CLIENT_IP_ADDRESS_CTX_KEY.get();
      if (clientIpAddress != null) {
        clientMachine = clientIpAddress;
      } else {
        clientMachine = "";
      }
    }
    return clientMachine;
  }

  @Override
  public AuditMessage buildAuditMessageForSuccess(AuditAction op,
      Map<String, String> auditMap) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(getClientAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }

  @Override
  public AuditMessage buildAuditMessageForFailure(AuditAction op,
      Map<String, String> auditMap, Throwable throwable) {

    return new AuditMessage.Builder()
        .setUser(getRemoteUserName())
        .atIp(getClientAddress())
        .forOperation(op)
        .withParams(auditMap)
        .withResult(AuditEventStatus.FAILURE)
        .withException(throwable)
        .build();
  }

  /**
   * Returns true if OzoneNativeAuthorizer is enabled and false if otherwise.
   *
   * @return if native authorizer is enabled.
   */
  public boolean isNativeAuthorizerEnabled() {
    return accessAuthorizer.isNative();
  }

  private ResourceType getResourceType(OmKeyArgs args) {
    if (args.getKeyName() == null || args.getKeyName().isEmpty()) {
      return ResourceType.BUCKET;
    }
    return ResourceType.KEY;
  }

}
