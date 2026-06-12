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

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneConfigUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.TransitionBuilder;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.lock.OBSKeyLockManager;
import org.apache.hadoop.ozone.om.request.PlannedRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

/**
 * OBS CreateKey in the planned execution path.
 * Acquires only bucket read lock (openKeyTable entries are unique per clientID).
 */
public final class OBSCreateKeyPlannedRequest extends PlannedRequest {

  public OBSCreateKeyPlannedRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public void preProcess(OzoneManager om) throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
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
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(
        getOmRequest().getUserInfo().getUserName());
    om.checkAcls(ResourceType.KEY, StoreType.OZONE, ACLType.CREATE,
        keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), ugi, null, null, true, null);
  }

  @Override
  public void acquireLocks(OzoneManager om) throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    OBSKeyLockManager lockManager = om.getKeyLockManager();
    lockManager.acquireBucketReadLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName());
  }

  @Override
  public void releaseLocks(OzoneManager om) {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    OBSKeyLockManager lockManager = om.getKeyLockManager();
    lockManager.releaseBucketReadLock(keyArgs.getVolumeName(),
        keyArgs.getBucketName());
  }

  @Override
  public void plan(OzoneManager om, TransitionBuilder builder)
      throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();
    long clientID = createKeyRequest.getClientID();

    OMMetadataManager metadataManager = om.getMetadataManager();
    long managedIndex = builder.getManagedIndex();

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = metadataManager.getBucketTable().get(bucketKey);
    if (bucketInfo == null) {
      throw new OMException("Bucket not found: " + volumeName + "/" + bucketName,
          OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

    ReplicationConfig replicationConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(keyArgs.getType(),
            keyArgs.getFactor(), keyArgs.getEcReplicationConfig(),
            bucketInfo.getDefaultReplicationConfig(), om);

    long dataSize = keyArgs.getDataSize() > 0 ? keyArgs.getDataSize() : 0;
    List<OmKeyLocationInfo> locationInfoList;
    if (dataSize > 0 && !keyArgs.getIsMultipartKey()) {
      locationInfoList = keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());
    } else {
      locationInfoList = Collections.emptyList();
    }

    long objectId = managedIndex;
    OmKeyInfo.Builder keyInfoBuilder = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(
            Collections.singletonList(
                new OmKeyLocationInfoGroup(0, locationInfoList)))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(dataSize)
        .setReplicationConfig(replicationConfig)
        .setObjectID(objectId)
        .setUpdateID(managedIndex);
    keyArgs.getMetadataList().forEach(
        kv -> keyInfoBuilder.addMetadata(kv.getKey(), kv.getValue()));
    OmKeyInfo omKeyInfo = keyInfoBuilder.build();

    // Update bucket quota
    long preAllocatedSpace = QuotaUtil.getReplicatedSize(
        dataSize, replicationConfig);
    bucketInfo.incrUsedNamespace(1);
    bucketInfo.incrUsedBytes(preAllocatedSpace);

    // Write to openKeyTable
    String openKeyName = metadataManager.getOpenKey(volumeName, bucketName,
        keyName, String.valueOf(clientID));
    builder.put(OPEN_KEY_TABLE, openKeyName, omKeyInfo,
        OmKeyInfo.getCodec());

    // Write updated bucket info
    builder.put(BUCKET_TABLE, bucketKey, bucketInfo,
        OmBucketInfo.getCodec());

    // Build response
    OMResponse response = OMResponse.newBuilder()
        .setCmdType(getCmdType())
        .setStatus(Status.OK)
        .setSuccess(true)
        .setCreateKeyResponse(CreateKeyResponse.newBuilder()
            .setKeyInfo(omKeyInfo.getNetworkProtobuf(
                getOmRequest().getVersion(), true))
            .setOpenVersion(0))
        .build();
    builder.setResponse(response);
  }
}
