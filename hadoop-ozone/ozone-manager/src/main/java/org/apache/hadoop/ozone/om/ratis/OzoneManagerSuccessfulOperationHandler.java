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

package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OperationInfo;
import org.apache.hadoop.ozone.om.helpers.OperationInfo.OperationArgs;
import org.apache.hadoop.ozone.om.helpers.OperationInfo.OperationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a simple hook on a successful write operation.  It's
 * only purpose at the moment is to write an OperationInfo record to the DB
 */
public final class OzoneManagerSuccessfulOperationHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerSuccessfulOperationHandler.class);

  private final OzoneManager ozoneManager;

  public OzoneManagerSuccessfulOperationHandler(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
  }

  public void handle(long trxLogIndex, OzoneManagerProtocolProtos.OMRequest omRequest) {

    switch (omRequest.getCmdType()) {
      case CreateKey:
        logRequest("CreateKey", omRequest);
        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getCreateKeyRequest().getKeyArgs(),
            new OperationArgs.CreateKeyArgs()));
        break;
      case RenameKey:
        logRequest("RenameKey", omRequest);
        OzoneManagerProtocolProtos.RenameKeyRequest renameReq
            = (OzoneManagerProtocolProtos.RenameKeyRequest) omRequest.getRenameKeyRequest();

        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getRenameKeyRequest().getKeyArgs(),
            new OperationArgs.RenameKeyArgs(renameReq.getToKeyName())));

        break;
      case DeleteKey:
        logRequest("DeleteKey", omRequest);
        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getDeleteKeyRequest().getKeyArgs(),
            new OperationArgs.DeleteKeyArgs()));
        break;
      case CommitKey:
        logRequest("CommitKey", omRequest);
        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getCommitKeyRequest().getKeyArgs(),
            new OperationArgs.CommitKeyArgs()));
        break;
      case CreateDirectory:
        logRequest("CreateDirectory", omRequest);
        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getCreateDirectoryRequest().getKeyArgs(),
            new OperationArgs.CreateDirectoryArgs()));
        break;
      case CreateFile:
        logRequest("CreateFile", omRequest);

        OzoneManagerProtocolProtos.CreateFileRequest createFileReq
            = (OzoneManagerProtocolProtos.CreateFileRequest) omRequest.getCreateFileRequest();

        storeOperationInfo(buildOperationInfo(trxLogIndex,
            omRequest.getCreateFileRequest().getKeyArgs(),
            new OperationArgs.CreateFileArgs(createFileReq.getIsRecursive(),
                                             createFileReq.getIsOverwrite())));
        break;
      default:
        LOG.error("Unhandled cmdType={}", omRequest.getCmdType());
        break;
    }
  }

  private static void logRequest(String label, OzoneManagerProtocolProtos.OMRequest omRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("---> {} {}", label, omRequest);
    }
  }

  private OperationInfo buildOperationInfo(long trxLogIndex,
                                           OzoneManagerProtocolProtos.KeyArgs keyArgs,
                                           OperationArgs opArgs) {
    return OperationInfo.newBuilder()
        .setTrxLogIndex(trxLogIndex)
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setCreationTime(System.currentTimeMillis())
        .setOpArgs(opArgs)
        .build();
  }

  private void storeOperationInfo(OperationInfo operationInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing operation {}", operationInfo);
    }

    String key = operationInfo.getDbKey();

    // XXX: should this be part of an atomic db txn that happens at the end
    // of each replayed event (so that the ledger is consistent with the
    // processed ratis events)

    try {
      ozoneManager.getMetadataManager().getOperationInfoTable().put(key, operationInfo);
    //} catch (IOException ex) {
    //  LOG.error("Unable to write operation {}", operationInfo, ex);
    } catch (Exception ex) {
      LOG.error("Unable to write operation {}", operationInfo, ex);
    }
  }
}
