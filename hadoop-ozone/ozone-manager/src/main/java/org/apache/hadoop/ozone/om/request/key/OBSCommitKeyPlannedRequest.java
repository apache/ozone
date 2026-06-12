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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.TransitionBuilder;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.lock.OBSKeyLockManager;
import org.apache.hadoop.ozone.om.request.PlannedRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

/**
 * OBS CommitKey in the planned execution path.
 * Acquires bucket read lock + striped key write lock.
 */
public final class OBSCommitKeyPlannedRequest extends PlannedRequest {

  public OBSCommitKeyPlannedRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public void preProcess(OzoneManager om) throws IOException {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();
    if (keyArgs.getKeyName().length() == 0) {
      throw new OMException("Key name is empty",
          OMException.ResultCodes.INVALID_KEY_NAME);
    }
  }

  @Override
  public void authorize(OzoneManager om) throws IOException {
    if (!om.getAclsEnabled()) {
      return;
    }
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(
        getOmRequest().getUserInfo().getUserName());
    om.checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.WRITE,
        keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), ugi, null, null, true, null);
  }

  @Override
  public void acquireLocks(OzoneManager om) throws IOException {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();
    OBSKeyLockManager lockManager = om.getKeyLockManager();
    lockManager.acquireBucketReadLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName());
    lockManager.acquireKeyWriteLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName(), keyArgs.getKeyName());
  }

  @Override
  public void releaseLocks(OzoneManager om) {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();
    OBSKeyLockManager lockManager = om.getKeyLockManager();
    lockManager.releaseKeyWriteLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName(), keyArgs.getKeyName());
    lockManager.releaseBucketReadLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName());
  }

  @Override
  public void plan(OzoneManager om, TransitionBuilder builder)
      throws IOException {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long clientID = commitKeyRequest.getClientID();
    long managedIndex = builder.getManagedIndex();

    OMMetadataManager metadataManager = om.getMetadataManager();

    // Validate bucket exists
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = metadataManager.getBucketTable().get(bucketKey);
    if (bucketInfo == null) {
      throw new OMException("Bucket not found: " + volumeName + "/" + bucketName,
          OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

    // Read open key
    String openKeyName = metadataManager.getOpenKey(volumeName, bucketName,
        keyName, String.valueOf(clientID));
    OmKeyInfo omKeyInfo = metadataManager.getOpenKeyTable(BucketLayout.OBJECT_STORE)
        .get(openKeyName);
    if (omKeyInfo == null) {
      throw new OMException("Failed to commit key, as " + openKeyName +
          " entry is not found in the OpenKey table",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }

    // Update locations from the commit request
    List<OmKeyLocationInfo> locationInfoList = keyArgs.getKeyLocationsList()
        .stream()
        .map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());

    omKeyInfo.appendNewBlocks(locationInfoList, false);
    omKeyInfo.setModificationTime(keyArgs.getModificationTime());
    omKeyInfo.setDataSize(keyArgs.getDataSize());

    // Check for overwrite — move existing key to deleted table
    String ozoneKey = metadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    OmKeyInfo existingKey = metadataManager.getKeyTable(BucketLayout.OBJECT_STORE)
        .get(ozoneKey);

    long correctedUsedBytes = 0;
    if (existingKey != null) {
      RepeatedOmKeyInfo deletedKeys = new RepeatedOmKeyInfo(
          existingKey, bucketInfo.getObjectID());
      builder.put(DELETED_TABLE, ozoneKey, deletedKeys,
          RepeatedOmKeyInfo.getCodec(true));
      correctedUsedBytes = QuotaUtil.getReplicatedSize(
          existingKey.getDataSize(), existingKey.getReplicationConfig());
    }

    // Delete from openKeyTable
    builder.delete(OPEN_KEY_TABLE, openKeyName);

    // Put in keyTable
    builder.put(KEY_TABLE, ozoneKey, omKeyInfo, OmKeyInfo.getCodec(true));

    // Update bucket quota: on overwrite, subtract old key size
    if (existingKey != null) {
      bucketInfo.incrUsedBytes(-correctedUsedBytes);
      bucketInfo.incrUsedNamespace(-1);
    }
    builder.put(BUCKET_TABLE, bucketKey, bucketInfo,
        OmBucketInfo.getCodec());

    // Build response
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(getCmdType())
        .setStatus(Status.OK)
        .setSuccess(true)
        .build();
    builder.setResponse(response);
  }
}
