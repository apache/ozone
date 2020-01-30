/**
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

package org.apache.hadoop.ozone.om.request.volume;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeSetOwnerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

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
    SetVolumePropertyRequest modifiedRequest = getOmRequest()
        .getSetVolumePropertyRequest().toBuilder()
        .setModificationTime(modificationTime).build();

    return getOmRequest().toBuilder()
        .setSetVolumePropertyRequest(modifiedRequest.toBuilder())
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    SetVolumePropertyRequest setVolumePropertyRequest =
        getOmRequest().getSetVolumePropertyRequest();
    Preconditions.checkNotNull(setVolumePropertyRequest);

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
    IOException exception = null;
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
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
      OzoneManagerProtocolProtos.UserVolumeInfo oldOwnerVolumeList = null;
      OzoneManagerProtocolProtos.UserVolumeInfo newOwnerVolumeList = null;
      OmVolumeArgs omVolumeArgs = null;

      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume);
      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);
      if (omVolumeArgs == null) {
        LOG.debug("Changing volume ownership failed for user:{} volume:{}",
            newOwner, volume);
        throw new OMException("Volume " + volume + " is not found",
            OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      // Check if this transaction is a replay of ratis logs.
      // If this is a replay, then the response has already been returned to
      // the client. So take no further action and return a dummy
      // OMClientResponse.
      if (isReplay(ozoneManager, omVolumeArgs, transactionLogIndex)) {
        LOG.debug("Replayed Transaction {} ignored. Request: {}",
            transactionLogIndex, setVolumePropertyRequest);
        return new OMVolumeSetOwnerResponse(createReplayOMResponse(omResponse));
      }

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
        // Note: addResponseToDoubleBuffer would be executed in finally block.
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
      omVolumeArgs.setOwnerName(newOwner);
      omVolumeArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());

      // Update modificationTime.
      omVolumeArgs.setModificationTime(
          setVolumePropertyRequest.getModificationTime());

      // Update cache.
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(newOwner)),
          new CacheValue<>(Optional.of(newOwnerVolumeList),
              transactionLogIndex));
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(oldOwner)),
          new CacheValue<>(Optional.of(oldOwnerVolumeList),
              transactionLogIndex));
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

      omResponse.setSetVolumePropertyResponse(
          SetVolumePropertyResponse.newBuilder().setResponse(true).build());
      omClientResponse = new OMVolumeSetOwnerResponse(omResponse.build(),
          oldOwner, oldOwnerVolumeList, newOwnerVolumeList, omVolumeArgs);

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMVolumeSetOwnerResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      addResponseToDoubleBuffer(transactionLogIndex, omClientResponse,
          ozoneManagerDoubleBufferHelper);
      if (acquiredUserLocks) {
        omMetadataManager.getLock().releaseMultiUserLock(newOwner, oldOwner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volume);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_OWNER, auditMap,
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

