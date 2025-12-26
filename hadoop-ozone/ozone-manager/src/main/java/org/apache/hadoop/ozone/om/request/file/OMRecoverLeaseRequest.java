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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.READ;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto.WRITE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_CLOSED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.HBASE_SUPPORT;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.RecoverLease;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenSecretManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmFSOFile;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMRecoverLeaseResponse;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverLeaseResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform actions for RecoverLease requests.
 */
public class OMRecoverLeaseRequest extends OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMRecoverLeaseRequest.class);

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OmKeyInfo openKeyInfo;
  private String dbOpenFileKey;
  private boolean force;

  private OMMetadataManager omMetadataManager;

  public OMRecoverLeaseRequest(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();

    Objects.requireNonNull(recoverLeaseRequest, "recoverLeaseRequest == null");
    volumeName = recoverLeaseRequest.getVolumeName();
    bucketName = recoverLeaseRequest.getBucketName();
    keyName = recoverLeaseRequest.getKeyName();
    force = recoverLeaseRequest.getForce();
  }

  @Override
  @DisallowedUntilLayoutVersion(HBASE_SUPPORT)
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    OMRequest request = super.preExecute(ozoneManager);
    RecoverLeaseRequest recoverLeaseRequest = request.getRecoverLeaseRequest();

    String keyPath = recoverLeaseRequest.getKeyName();
    String normalizedKeyPath =
        validateAndNormalizeKey(ozoneManager.getEnableFileSystemPaths(),
            keyPath, getBucketLayout());

    // check ACL
    checkKeyAcls(ozoneManager,
        recoverLeaseRequest.getVolumeName(),
        recoverLeaseRequest.getBucketName(),
        recoverLeaseRequest.getKeyName(),
        IAccessAuthorizer.ACLType.WRITE, OzoneObj.ResourceType.KEY);

    return request.toBuilder()
        .setRecoverLeaseRequest(
            recoverLeaseRequest.toBuilder()
                .setKeyName(normalizedKeyPath))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    RecoverLeaseRequest recoverLeaseRequest = getOmRequest()
        .getRecoverLeaseRequest();
    Objects.requireNonNull(recoverLeaseRequest, "recoverLeaseRequest == null");

    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);
    auditMap.put(OzoneConsts.KEY, keyName);

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    omMetadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    Exception exception = null;
    // increment metric
    OMMetrics omMetrics = ozoneManager.getMetrics();

    boolean acquiredLock = false;
    try {
      // acquire lock
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
              volumeName, bucketName));
      acquiredLock = getOmLockDetails().isLockAcquired();
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

      RecoverLeaseResponse recoverLeaseResponse = doWork(ozoneManager, context.getIndex());

      // Prepare response
      omResponse.setRecoverLeaseResponse(recoverLeaseResponse).setCmdType(RecoverLease);
      omClientResponse = new OMRecoverLeaseResponse(omResponse.build(), getBucketLayout(),
          dbOpenFileKey, openKeyInfo);
      omMetrics.incNumRecoverLease();
      LOG.debug("Key recovered. Volume:{}, Bucket:{}, Key:{}",
          volumeName, bucketName, keyName);
    } catch (IOException | InvalidPathException ex) {
      LOG.error("Fail for recovering lease. Volume:{}, Bucket:{}, Key:{}",
          volumeName, bucketName, keyName, ex);
      exception = ex;
      omMetrics.incNumRecoverLeaseFails();
      omClientResponse = new OMRecoverLeaseResponse(
          createErrorOMResponse(omResponse, exception), getBucketLayout());
    } finally {
      if (acquiredLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK,
                volumeName, bucketName));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Audit Log outside the lock
    markForAudit(ozoneManager.getAuditLogger(), buildAuditMessage(
        OMAction.RECOVER_LEASE, auditMap, exception,
        getOmRequest().getUserInfo()));

    return omClientResponse;
  }

  private RecoverLeaseResponse doWork(OzoneManager ozoneManager,
      long transactionLogIndex) throws IOException {

    String errMsg = "Cannot recover file : " + keyName
        + " as parent directory doesn't exist";

    OmFSOFile fsoFile =  new OmFSOFile.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmMetadataManager(omMetadataManager)
        .setErrMsg(errMsg)
        .build();

    String dbFileKey = fsoFile.getOzonePathKey();

    OmKeyInfo keyInfo = getKey(dbFileKey);
    if (keyInfo == null) {
      throw new OMException("Key:" + keyName + " not found in keyTable.", KEY_NOT_FOUND);
    }

    final String writerId = keyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
    if (writerId == null) {
      // if file is closed, do nothing and return right away.
      throw new OMException("Key: " + keyName + " is already closed", KEY_ALREADY_CLOSED);
    }

    dbOpenFileKey = fsoFile.getOpenFileName(Long.parseLong(writerId));
    openKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout()).get(dbOpenFileKey);
    if (openKeyInfo == null) {
      throw new OMException("Open Key " + dbOpenFileKey + " not found in openKeyTable", KEY_NOT_FOUND);
    }

    if (openKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)) {
      throw new OMException("Open Key " + keyName + " is already deleted",
          KEY_NOT_FOUND);
    }
    if (openKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY)) {
      LOG.debug("Key: " + keyName + " is already under recovery");
    } else {
      final long leaseSoftLimit = ozoneManager.getConfiguration()
          .getTimeDuration(OZONE_OM_LEASE_SOFT_LIMIT, OZONE_OM_LEASE_SOFT_LIMIT_DEFAULT, TimeUnit.MILLISECONDS);
      if (!force && Time.now() < openKeyInfo.getModificationTime() + leaseSoftLimit) {
        throw new OMException("Open Key " + keyName + " updated recently and is inside soft limit period",
            KEY_UNDER_LEASE_SOFT_LIMIT_PERIOD);
      }
      openKeyInfo = openKeyInfo.toBuilder()
          .addMetadata(OzoneConsts.LEASE_RECOVERY, "true")
          .setUpdateID(transactionLogIndex)
          .build();
      openKeyInfo.setModificationTime(Time.now());
      // add to cache.
      omMetadataManager.getOpenKeyTable(getBucketLayout()).addCacheEntry(
          dbOpenFileKey, openKeyInfo, transactionLogIndex);
    }
    // override key name with normalizedKeyPath
    keyInfo.setKeyName(keyName);
    openKeyInfo.setKeyName(keyName);

    OmKeyLocationInfoGroup keyLatestVersionLocations = keyInfo.getLatestVersionLocations();
    List<OmKeyLocationInfo> keyLocationInfoList = keyLatestVersionLocations.getLocationList();
    OmKeyLocationInfoGroup openKeyLatestVersionLocations = openKeyInfo.getLatestVersionLocations();
    List<OmKeyLocationInfo> openKeyLocationInfoList = openKeyLatestVersionLocations.getLocationList();

    if (!keyLocationInfoList.isEmpty()) {
      updateBlockInfo(ozoneManager, keyLocationInfoList.get(keyLocationInfoList.size() - 1));
    }
    if (openKeyLocationInfoList.size() > 1) {
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(openKeyLocationInfoList.size() - 1));
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(openKeyLocationInfoList.size() - 2));
    } else if (!openKeyLocationInfoList.isEmpty()) {
      updateBlockInfo(ozoneManager, openKeyLocationInfoList.get(0));
    }

    RecoverLeaseResponse.Builder rb = RecoverLeaseResponse.newBuilder();
    rb.setKeyInfo(keyInfo.getNetworkProtobuf(getOmRequest().getVersion(), true));
    rb.setOpenKeyInfo(openKeyInfo.getNetworkProtobuf(getOmRequest().getVersion(), true));
    return rb.build();
  }

  private void updateBlockInfo(OzoneManager ozoneManager, OmKeyLocationInfo blockInfo) throws IOException {
    if (blockInfo != null) {
      // set token to last block if enabled
      if (ozoneManager.isGrpcBlockTokenEnabled()) {
        String remoteUser = getRemoteUser().getShortUserName();
        OzoneBlockTokenSecretManager secretManager = ozoneManager.getBlockTokenSecretManager();
        blockInfo.setToken(secretManager.generateToken(remoteUser, blockInfo.getBlockID(),
            EnumSet.of(READ, WRITE), blockInfo.getLength()));
      }
      // refresh last block pipeline
      ContainerWithPipeline containerWithPipeline =
          ozoneManager.getScmClient().getContainerClient().getContainerWithPipeline(blockInfo.getContainerID());
      blockInfo.setPipeline(containerWithPipeline.getPipeline());
    }
  }

  private OmKeyInfo getKey(String dbOzoneKey) throws IOException {
    return omMetadataManager.getKeyTable(getBucketLayout()).get(dbOzoneKey);
  }
}
