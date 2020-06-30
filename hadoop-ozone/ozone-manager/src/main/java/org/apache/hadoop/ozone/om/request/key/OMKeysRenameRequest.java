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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmRenameKeyInfo;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
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
 * Handles rename keys request.
 */
public class OMKeysRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeysRenameRequest.class);

  public OMKeysRenameRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Stores the result of request execution for Rename Requests.
   */
  private enum Result {
    SUCCESS,
    DELETE_FROM_KEY_ONLY,
    REPLAY,
    FAILURE,
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    Preconditions.checkNotNull(renameKeysRequest);

    return getOmRequest().toBuilder()
        .setRenameKeysRequest(renameKeysRequest.toBuilder())
        .setUserInfo(getUserInfo()).build();

  }

  @Override
  @SuppressWarnings("methodlength")
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {

    RenameKeysRequest renameKeysRequest = getOmRequest().getRenameKeysRequest();
    OMClientResponse omClientResponse = null;
    Set<OmKeyInfo> unRenamedKeys = new HashSet<>();
    List<OmRenameKeyInfo> renameKeyInfoList = new ArrayList<>();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();


    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;

    Result result = null;
    Map<String, String> auditMap = null;
    RenameKeyRequest renameRequest = null;
    String toKey = null;
    String fromKey = null;
    String volumeName = null;
    String bucketName = null;
    String fromKeyName = null;
    String toKeyName = null;
    try {
      for (RenameKeyRequest renameKeyRequest : renameKeysRequest
          .getRenameKeyRequestList()) {
        OzoneManagerProtocolProtos.KeyArgs renameKeyArgs =
            renameKeyRequest.getKeyArgs();
        volumeName = renameKeyArgs.getVolumeName();
        bucketName = renameKeyArgs.getBucketName();
        fromKeyName = renameKeyArgs.getKeyName();
        String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            fromKeyName);
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);
        unRenamedKeys.add(omKeyInfo);
      }

      for (RenameKeyRequest renameKeyRequest : renameKeysRequest
          .getRenameKeyRequestList()) {
        OzoneManagerProtocolProtos.KeyArgs renameKeyArgs =
            renameKeyRequest.getKeyArgs();

        volumeName = renameKeyArgs.getVolumeName();
        bucketName = renameKeyArgs.getBucketName();
        fromKeyName = renameKeyArgs.getKeyName();
        toKeyName = renameKeyRequest.getToKeyName();
        auditMap = buildAuditMap(renameKeyArgs, renameKeyRequest);
        renameRequest = renameKeyRequest;

        if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
          throw new OMException("Key name is empty",
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
        // check Acls to see if user has access to perform delete operation on
        // old key and create operation on new key
        checkKeyAcls(ozoneManager, volumeName, bucketName, fromKeyName,
            IAccessAuthorizer.ACLType.DELETE, OzoneObj.ResourceType.KEY);
        checkKeyAcls(ozoneManager, volumeName, bucketName, toKeyName,
            IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

        // Validate bucket and volume exists or not.
        validateBucketAndVolume(omMetadataManager, volumeName, bucketName);

        // Check if toKey exists
        fromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
            fromKeyName);
        toKey =
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
        OmKeyInfo toKeyValue = omMetadataManager.getKeyTable().get(toKey);

        if (toKeyValue != null) {

          // Check if this transaction is a replay of ratis logs.
          if (isReplay(ozoneManager, toKeyValue, trxnLogIndex)) {

            // Check if fromKey is still in the DB and created before this
            // replay.
            // For example, lets say we have the following sequence of
            // transactions.
            //   Trxn 1 : Create Key1
            //   Trnx 2 : Rename Key1 to Key2 -> Deletes Key1 and Creates Key2
            // Now if these transactions are replayed:
            //   Replay Trxn 1 : Creates Key1 again it does not exist in DB
            //   Replay Trxn 2 : Key2 is not created as it exists in DB and
            //                   the request would be deemed a replay. But
            //                   Key1 is still in the DB and needs to be
            //                   deleted.
            fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
            if (fromKeyValue != null) {
              // Check if this replay transaction was after the fromKey was
              // created. If so, we have to delete the fromKey.
              if (ozoneManager.isRatisEnabled() &&
                  trxnLogIndex > fromKeyValue.getUpdateID()) {
                // Add to cache. Only fromKey should be deleted. ToKey already
                // exists in DB as this transaction is a replay.
                result = Result.DELETE_FROM_KEY_ONLY;
                renameKeyInfoList.add(new OmRenameKeyInfo(fromKeyName,
                    null, fromKeyValue));
              }
            }

            if (result == null) {
              result = Result.REPLAY;
              // If toKey exists and fromKey does not, then no further action is
              // required. Return a dummy OMClientResponse.
              omClientResponse =
                  new OMKeysRenameResponse(createReplayOMResponse(
                      omResponse));
            }
          } else {
            // This transaction is not a replay. toKeyName should not exist
            throw new OMException("Key already exists " + toKeyName,
                OMException.ResultCodes.KEY_ALREADY_EXISTS);
          }
        } else {
          // fromKeyName should exist
          fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
          if (fromKeyValue == null) {
            // TODO: Add support for renaming open key
            throw new OMException("Key not found " + fromKey, KEY_NOT_FOUND);
          }

          fromKeyValue.setUpdateID(trxnLogIndex, ozoneManager.isRatisEnabled());

          fromKeyValue.setKeyName(toKeyName);
          //Set modification time
          fromKeyValue.setModificationTime(renameKeyArgs.getModificationTime());

          renameKeyInfoList
              .add(new OmRenameKeyInfo(fromKeyName, toKeyName, fromKeyValue));
        }
      }
      omClientResponse = new OMKeysRenameResponse(omResponse
          .setRenameKeysResponse(RenameKeysResponse.newBuilder()).build(),
          renameKeyInfoList, trxnLogIndex);
      result = Result.SUCCESS;
    } catch (IOException ex) {
      result = Result.FAILURE;
      exception = ex;
      omClientResponse = new OMKeyDeleteResponse(
          createRenameKeysErrorOMResponse(omResponse, exception,
              unRenamedKeys));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    if (result == Result.SUCCESS || result == Result.FAILURE) {
      auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
          exception, getOmRequest().getUserInfo()));
    }

    switch (result) {
    case SUCCESS:
      LOG.debug("Rename Key is successfully completed for volume:{} bucket:{}"
              + " fromKey:{} toKey:{}. ", volumeName, bucketName, fromKeyName,
          toKeyName);
      break;
    case DELETE_FROM_KEY_ONLY:
      LOG.debug("Replayed transaction {}: {}. Renamed Key {} already exists. "
              + "Deleting old key {}.", trxnLogIndex, renameRequest, toKey,
          fromKey);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
          renameRequest);
      break;
    case FAILURE:
      ozoneManager.getMetrics().incNumKeyRenameFails();
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} " +
              "toKey:{}. Key: {} not found.", volumeName, bucketName,
          fromKeyName, toKeyName, fromKeyName);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyRenameRequest: {}",
          renameRequest);
    }

    return omClientResponse;
  }

  private Map<String, String> buildAuditMap(
      KeyArgs keyArgs, RenameKeyRequest renameKeyRequest) {
    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    auditMap.remove(OzoneConsts.KEY);
    auditMap.put(OzoneConsts.SRC_KEY, keyArgs.getKeyName());
    auditMap.put(OzoneConsts.DST_KEY, renameKeyRequest.getToKeyName());
    return auditMap;
  }
}
