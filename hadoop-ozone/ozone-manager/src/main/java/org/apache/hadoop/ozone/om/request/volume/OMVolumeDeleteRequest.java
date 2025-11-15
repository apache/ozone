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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles volume delete request.
 */
public class OMVolumeDeleteRequest extends OMVolumeRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeDeleteRequest.class);

  public OMVolumeDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    super.preExecute(ozoneManager);
    DeleteVolumeRequest deleteVolumeRequest =
        getOmRequest().getDeleteVolumeRequest();
    Preconditions.checkNotNull(deleteVolumeRequest);
    String volume = deleteVolumeRequest.getVolumeName();

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE, volume,
            null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        Map<String, String> auditMap = new LinkedHashMap<>();
        auditMap.put(OzoneConsts.VOLUME, volume);
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.DELETE_VOLUME, auditMap, ex,
                getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    return getOmRequest().toBuilder()
        .setUserInfo(getUserIfNotExists(ozoneManager))
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    DeleteVolumeRequest deleteVolumeRequest =
        getOmRequest().getDeleteVolumeRequest();
    Preconditions.checkNotNull(deleteVolumeRequest);

    String volume = deleteVolumeRequest.getVolumeName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeDeletes();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredUserLock = false;
    boolean acquiredVolumeLock = false;
    Exception exception = null;
    String owner = null;
    OMClientResponse omClientResponse = null;
    try {
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      OmVolumeArgs omVolumeArgs = getVolumeInfo(omMetadataManager, volume);

      // Check reference count
      final long volRefCount = omVolumeArgs.getRefCount();
      if (volRefCount != 0L) {
        LOG.debug("volume: {} has a non-zero ref count. won't delete", volume);
        throw new OMException("Volume reference count is not zero (" +
            volRefCount + "). Ozone features are enabled on this volume. " +
            "Try `ozone tenant delete <tenantId>` first.",
            OMException.ResultCodes.VOLUME_IS_REFERENCED);
      }

      owner = omVolumeArgs.getOwnerName();
      mergeOmLockDetails(
          omMetadataManager.getLock().acquireWriteLock(USER_LOCK, owner));
      acquiredUserLock = getOmLockDetails().isLockAcquired();

      String dbUserKey = omMetadataManager.getUserKey(owner);
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);

      if (!omMetadataManager.isVolumeEmpty(volume)) {
        LOG.debug("volume:{} is not empty", volume);
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_EMPTY);
      }

      OzoneManagerStorageProtos.PersistedUserVolumeInfo newVolumeList =
          omMetadataManager.getUserTable().get(owner);

      // delete the volume from the owner list
      // as well as delete the volume entry
      newVolumeList = delVolumeFromOwnerList(newVolumeList, volume, owner,
          transactionLogIndex);

      omMetadataManager.getUserTable().addCacheEntry(new CacheKey<>(dbUserKey),
          CacheValue.get(transactionLogIndex, newVolumeList));

      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey), CacheValue.get(transactionLogIndex));

      omResponse.setDeleteVolumeResponse(
          DeleteVolumeResponse.newBuilder().build());
      omClientResponse = new OMVolumeDeleteResponse(omResponse.build(),
          volume, owner, newVolumeList);

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMVolumeDeleteResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredUserLock) {
        mergeOmLockDetails(omMetadataManager.getLock()
            .releaseWriteLock(USER_LOCK, owner));
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
    markForAudit(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_VOLUME, buildVolumeAuditMap(volume),
            exception, getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Volume deleted for user:{} volume:{}", owner, volume);
      omMetrics.decNumVolumes();
    } else {
      LOG.error("Volume deletion failed for user:{} volume:{}",
          owner, volume, exception);
      omMetrics.incNumVolumeDeleteFails();
    }
    return omClientResponse;
  }

}

