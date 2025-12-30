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

package org.apache.hadoop.ozone.om.request.volume;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeSetOwnerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle set owner request for volume.
 */
public class OMVolumeSetOwnerRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetOwnerRequest.class);

  public OMVolumeSetOwnerRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    long modificationTime = Time.now();
    SetVolumePropertyRequest.Builder setPropertyRequestBuilder = getOmRequest()
        .getSetVolumePropertyRequest().toBuilder()
        .setModificationTime(modificationTime);

    return getOmRequest().toBuilder()
        .setSetVolumePropertyRequest(setPropertyRequestBuilder)
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();
    SetVolumePropertyRequest setVolumePropertyRequest =
        getOmRequest().getSetVolumePropertyRequest();
    Objects.requireNonNull(setVolumePropertyRequest, "setVolumePropertyRequest == null");

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    // In production this will never happen, this request will be called only
    // when we have ownerName in setVolumePropertyRequest.
    if (!setVolumePropertyRequest.hasOwnerName()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMVolumeSetOwnerResponse(omResponse.build());
    }

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    String volume = setVolumePropertyRequest.getVolumeName();
    String newOwner = setVolumePropertyRequest.getOwnerName();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildVolumeAuditMap(volume);
    auditMap.put(OzoneConsts.OWNER, newOwner);

    boolean acquiredUserLocks = false;
    boolean acquiredVolumeLock = false;
    Exception exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String oldOwner = null;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      }

      long maxUserVolumeCount = ozoneManager.getMaxUserVolumeCount();
      OzoneManagerStorageProtos.PersistedUserVolumeInfo oldOwnerVolumeList;
      OzoneManagerStorageProtos.PersistedUserVolumeInfo newOwnerVolumeList;
      OmVolumeArgs omVolumeArgs = null;

      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();
      omVolumeArgs = getVolumeInfo(omMetadataManager, volume);
      oldOwner = omVolumeArgs.getOwnerName();

      // Return OK immediately if newOwner is the same as oldOwner.
      if (oldOwner.equals(newOwner)) {
        LOG.warn("Volume '{}' owner is already user '{}'.", volume, oldOwner);
        omResponse.setStatus(OzoneManagerProtocolProtos.Status.OK)
          .setMessage(
            "Volume '" + volume + "' owner is already '" + newOwner + "'.")
            .setSuccess(false);
        omResponse.setSetVolumePropertyResponse(
            SetVolumePropertyResponse.newBuilder().setResponse(false).build());
        omClientResponse = new OMVolumeSetOwnerResponse(omResponse.build());
        return omClientResponse;
      }

      acquiredUserLocks =
          omMetadataManager.getLock().acquireMultiUserLock(newOwner, oldOwner);
      oldOwnerVolumeList =
          omMetadataManager.getUserTable().get(oldOwner);
      oldOwnerVolumeList = delVolumeFromOwnerList(
          oldOwnerVolumeList, volume, oldOwner, transactionLogIndex);
      newOwnerVolumeList = omMetadataManager.getUserTable().get(newOwner);
      newOwnerVolumeList = addVolumeToOwnerList(
          newOwnerVolumeList, volume, newOwner,
          maxUserVolumeCount, transactionLogIndex);

      // Set owner with new owner name.
      omVolumeArgs = omVolumeArgs.toBuilder()
          .setOwnerName(newOwner)
          .setModificationTime(setVolumePropertyRequest.getModificationTime())
          .setUpdateID(transactionLogIndex)
          .build();

      // Update cache.
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(newOwner)),
          CacheValue.get(transactionLogIndex, newOwnerVolumeList));
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(oldOwner)),
          CacheValue.get(transactionLogIndex, oldOwnerVolumeList));
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getVolumeKey(volume)),
          CacheValue.get(transactionLogIndex, omVolumeArgs));

      omResponse.setSetVolumePropertyResponse(
          SetVolumePropertyResponse.newBuilder().setResponse(true).build());
      omClientResponse = new OMVolumeSetOwnerResponse(omResponse.build(),
          oldOwner, oldOwnerVolumeList, newOwnerVolumeList, omVolumeArgs);
    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMVolumeSetOwnerResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredUserLocks) {
        omMetadataManager.getLock().releaseMultiUserLock(newOwner, oldOwner);
      }
      if (acquiredVolumeLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(VOLUME_LOCK, volume));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(auditLogger, buildAuditMessage(OMAction.SET_OWNER, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Successfully changed Owner of Volume {} from {} -> {}", volume,
          oldOwner, newOwner);
    } else {
      LOG.error("Changing volume ownership failed for user:{} volume:{}",
          newOwner, volume, exception);
      omMetrics.incNumVolumeUpdateFails();
    }
    return omClientResponse;
  }

}

