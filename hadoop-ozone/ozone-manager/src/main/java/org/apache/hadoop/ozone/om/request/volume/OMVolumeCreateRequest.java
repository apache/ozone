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
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserVolumeInfo;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;

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

    VolumeInfo volumeInfo  =
        getOmRequest().getCreateVolumeRequest().getVolumeInfo();
    // Verify resource name
    OmUtils.validateVolumeName(volumeInfo.getVolume());

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    CreateVolumeRequest createVolumeRequest =
        getOmRequest().getCreateVolumeRequest();
    Preconditions.checkNotNull(createVolumeRequest);
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
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    OmVolumeArgs omVolumeArgs = null;
    Map<String, String> auditMap = new HashMap<>();
    try {
      omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      // when you create a volume, we set both Object ID and update ID.
      // The Object ID will never change, but update
      // ID will be set to transactionID each time we update the object.
      omVolumeArgs.setObjectID(
          OMFileRequest.getObjIDFromTxId(transactionLogIndex));
      omVolumeArgs.setUpdateID(transactionLogIndex,
          ozoneManager.isRatisEnabled());


      auditMap = omVolumeArgs.toAuditMap();

      // check acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE, volume,
            null, null);
      }

      // acquire lock.
      acquiredVolumeLock = omMetadataManager.getLock().acquireWriteLock(
          VOLUME_LOCK, volume);

      acquiredUserLock = omMetadataManager.getLock().acquireWriteLock(USER_LOCK,
          owner);

      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);

      OmVolumeArgs dbVolumeArgs =
          omMetadataManager.getVolumeTable().get(dbVolumeKey);

      UserVolumeInfo volumeList = null;
      if (dbVolumeArgs == null) {
        String dbUserKey = omMetadataManager.getUserKey(owner);
        volumeList = omMetadataManager.getUserTable().get(dbUserKey);
        volumeList = addVolumeToOwnerList(volumeList, volume, owner,
            ozoneManager.getMaxUserVolumeCount(), transactionLogIndex);
        createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey, transactionLogIndex);

        omResponse.setCreateVolumeResponse(CreateVolumeResponse.newBuilder()
            .build());
        omClientResponse = new OMVolumeCreateResponse(omResponse.build(),
            omVolumeArgs, volumeList);
        LOG.debug("volume:{} successfully created", omVolumeArgs.getVolume());
      } else {
        // Check if this transaction is a replay of ratis logs.
        if (isReplay(ozoneManager, dbVolumeArgs, transactionLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy
          // OMClientResponse.
          LOG.debug("Replayed Transaction {} ignored. Request: {}",
              transactionLogIndex, createVolumeRequest);
          return new OMVolumeCreateResponse(createReplayOMResponse(omResponse));
        } else {
          LOG.debug("volume:{} already exists", omVolumeArgs.getVolume());
          throw new OMException("Volume already exists",
              OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
        }
      }

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMVolumeCreateResponse(
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseWriteLock(USER_LOCK, owner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseWriteLock(VOLUME_LOCK, volume);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
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


