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

package org.apache.hadoop.ozone.om.request.key;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

/**
 * Handles DeleteKey request.
 */
public class OMKeysDeleteRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysDeleteRequest.class);

  public OMKeysDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    DeleteKeysRequest deleteKeyRequest =
        getOmRequest().getDeleteKeysRequest();
    Preconditions.checkNotNull(deleteKeyRequest);
    List<KeyArgs> newKeyArgsList = new ArrayList<>();
    for (KeyArgs keyArgs : deleteKeyRequest.getKeyArgsList()) {
      newKeyArgsList.add(
          keyArgs.toBuilder().setModificationTime(Time.now()).build());
    }
    DeleteKeysRequest newDeleteKeyRequest = DeleteKeysRequest
        .newBuilder().addAllKeyArgs(newKeyArgsList).build();

    return getOmRequest().toBuilder()
        .setDeleteKeysRequest(newDeleteKeyRequest)
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    DeleteKeysRequest deleteKeyRequest =
        getOmRequest().getDeleteKeysRequest();

    List<KeyArgs> deleteKeyArgsList = deleteKeyRequest.getKeyArgsList();
    Set<OmKeyInfo> unDeletedKeys = new HashSet<>();
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    Result result = null;

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();
    Map<String, String> auditMap = null;
    String volumeName = "";
    String bucketName = "";
    String keyName = "";
    List<OmKeyInfo> omKeyInfoList = new ArrayList<>();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo =
        getOmRequest().getUserInfo();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    try {
      for (KeyArgs deleteKeyArgs : deleteKeyArgsList) {
        volumeName = deleteKeyArgs.getVolumeName();
        bucketName = deleteKeyArgs.getBucketName();
        keyName = deleteKeyArgs.getKeyName();
        String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            keyName);
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);
        omKeyInfoList.add(omKeyInfo);
        unDeletedKeys.add(omKeyInfo);
      }

      // Check if any of the key in the batch cannot be deleted. If exists the
      // batch will delete failed.
      for (KeyArgs deleteKeyArgs : deleteKeyArgsList) {
        volumeName = deleteKeyArgs.getVolumeName();
        bucketName = deleteKeyArgs.getBucketName();
        keyName = deleteKeyArgs.getKeyName();
        auditMap = buildKeyArgsAuditMap(deleteKeyArgs);
        // check Acl
        checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
            IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);

        String objectKey = omMetadataManager.getOzoneKey(
            volumeName, bucketName, keyName);

        // Validate bucket and volume exists or not.
        validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);

        if (omKeyInfo == null) {
          throw new OMException("Key not found: " + keyName, KEY_NOT_FOUND);
        }

        // Check if this transaction is a replay of ratis logs.
        if (isReplay(ozoneManager, omKeyInfo, trxnLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy
          // OMClientResponse.
          throw new OMReplayException();
        }
      }

      omClientResponse = new OMKeysDeleteResponse(omResponse
          .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()).build(),
          omKeyInfoList, trxnLogIndex, ozoneManager.isRatisEnabled());
      result = Result.SUCCESS;
    } catch (IOException ex) {
      if (ex instanceof OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new OMKeyDeleteResponse(createReplayOMResponse(
            omResponse));
      } else {
        result = Result.FAILURE;
        exception = ex;

        omClientResponse = new OMKeyDeleteResponse(
            createOperationKeysErrorOMResponse(omResponse, exception,
                unDeletedKeys));
      }

    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    // Performing audit logging outside of the lock.
    if (result != Result.REPLAY) {
      auditLog(auditLogger, buildAuditMessage(
          OMAction.DELETE_KEY, auditMap, exception, userInfo));
    }

    switch (result) {
    case SUCCESS:
      omMetrics.decNumKeys();
      LOG.debug("Key deleted. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}",
          trxnLogIndex, deleteKeyRequest);
      break;
    case FAILURE:
      omMetrics.incNumKeyDeleteFails();
      LOG.error("Key delete failed. Volume:{}, Bucket:{}, Key{}." +
          " Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyDeleteRequest: {}",
          deleteKeyRequest);
    }

    return omClientResponse;
  }
}
