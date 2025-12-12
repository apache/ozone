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

import static org.apache.hadoop.ozone.om.helpers.OzoneAclUtil.getDefaultAclList;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.VOLUME_LOCK;

import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles volume create request.
 */
public class OMVolumeCreateRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeCreateRequest.class);

  public OMVolumeCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    super.preExecute(ozoneManager);

    VolumeInfo volumeInfo  =
        getOmRequest().getCreateVolumeRequest().getVolumeInfo();
    // Verify resource name
    OmUtils.validateVolumeName(volumeInfo.getVolume(),
        ozoneManager.isStrictS3());

    // ACL check during preExecute
    if (ozoneManager.getAclsEnabled()) {
      try {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE,
            volumeInfo.getVolume(), null, null);
      } catch (IOException ex) {
        // Ensure audit log captures preExecute failures
        markForAudit(ozoneManager.getAuditLogger(),
            buildAuditMessage(OMAction.CREATE_VOLUME,
                buildVolumeAuditMap(volumeInfo.getVolume()), ex,
                getOmRequest().getUserInfo()));
        throw ex;
      }
    }

    // Set creation time & set modification time
    long initialTime = Time.now();
    VolumeInfo updatedVolumeInfo =
        volumeInfo.toBuilder()
            .setCreationTime(initialTime)
            .setModificationTime(initialTime)
            .build();

    return getOmRequest().toBuilder().setCreateVolumeRequest(
        CreateVolumeRequest.newBuilder().setVolumeInfo(updatedVolumeInfo))
        .setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
    final long transactionLogIndex = context.getIndex();

    CreateVolumeRequest createVolumeRequest =
        getOmRequest().getCreateVolumeRequest();
    Objects.requireNonNull(createVolumeRequest, "createVolumeRequest == null");
    VolumeInfo volumeInfo = createVolumeRequest.getVolumeInfo();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeCreates();

    String volume = volumeInfo.getVolume();
    String owner = volumeInfo.getOwnerName();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // Doing this here, so we can do protobuf conversion outside of lock.
    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    Exception exception = null;
    OMClientResponse omClientResponse = null;
    final OmVolumeArgs omVolumeArgs;
    Map<String, String> auditMap = null;
    try {
      // when you create a volume, we set both Object ID and update ID.
      // The Object ID will never change, but update
      // ID will be set to transactionID each time we update the object.
      OmVolumeArgs.Builder builder = OmVolumeArgs.builderFromProtobuf(volumeInfo)
          .setObjectID(ozoneManager.getObjectIdFromTxId(transactionLogIndex))
          .setUpdateID(transactionLogIndex);

      auditMap = builder.toAuditMap();
      // acquire lock.
      mergeOmLockDetails(omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume));
      acquiredVolumeLock = getOmLockDetails().isLockAcquired();

      mergeOmLockDetails(omMetadataManager.getLock()
          .acquireWriteLock(USER_LOCK, owner));
      acquiredUserLock = getOmLockDetails().isLockAcquired();

      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);

      PersistedUserVolumeInfo volumeList = null;
      if (omMetadataManager.getVolumeTable().isExist(dbVolumeKey)) {
        LOG.debug("volume:{} already exists", volume);
        throw new OMException("Volume already exists",
            OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
      } else {
        String dbUserKey = omMetadataManager.getUserKey(owner);
        volumeList = omMetadataManager.getUserTable().get(dbUserKey);
        volumeList = addVolumeToOwnerList(volumeList, volume, owner,
            ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);

        // Add default ACL for volume
        List<OzoneAcl> defaultAclList = getDefaultAclList(UserGroupInformation.createRemoteUser(owner),
            ozoneManager.getConfig());
        // ACLs from VolumeArgs
        if (ozoneManager.getConfig().ignoreClientACLs()) {
          builder.acls().set(defaultAclList);
        } else {
          builder.acls().addAll(defaultAclList);
        }

        omVolumeArgs = builder.build();

        createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey, transactionLogIndex);

        omResponse.setCreateVolumeResponse(CreateVolumeResponse.newBuilder()
            .build());
        omClientResponse = new OMVolumeCreateResponse(omResponse.build(),
            omVolumeArgs, volumeList);
        LOG.debug("volume:{} successfully created", omVolumeArgs.getVolume());
      }

    } catch (IOException | InvalidPathException ex) {
      exception = ex;
      omClientResponse = new OMVolumeCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (acquiredUserLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner));
      }
      if (acquiredVolumeLock) {
        mergeOmLockDetails(
            omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volume));
      }
      if (omClientResponse != null) {
        omClientResponse.setOmLockDetails(getOmLockDetails());
      }
    }

    // Performing audit logging outside of the lock.
    markForAudit(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_VOLUME, auditMap, exception,
            getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      LOG.info("created volume:{} for user:{}", volume, owner);
      omMetrics.incNumVolumes();
    } else {
      LOG.error("Volume creation failed for user:{} volume:{}", owner,
          volume, exception);
      omMetrics.incNumVolumeCreateFails();
    }
    return omClientResponse;
  }
}


