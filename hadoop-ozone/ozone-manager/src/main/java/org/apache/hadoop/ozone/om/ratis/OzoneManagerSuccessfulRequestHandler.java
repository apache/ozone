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
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationType;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a simple hook on a successful write operation.  It's
 * only purpose at the moment is to write an OmCompletedRequestInfo record to the DB
 */
public final class OzoneManagerSuccessfulRequestHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerSuccessfulRequestHandler.class);

  private final OzoneManager ozoneManager;
  private final OMMetadataManager omMetadataManager;

  public OzoneManagerSuccessfulRequestHandler(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    this.omMetadataManager = ozoneManager.getMetadataManager();
  }

  public void handle(long trxLogIndex, OzoneManagerProtocolProtos.OMRequest omRequest) {

    switch (omRequest.getCmdType()) {
      case CreateKey:
        logRequest("CreateKey", omRequest);
        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
            omRequest.getCreateKeyRequest().getKeyArgs(),
            new OperationArgs.CreateKeyArgs()));
        break;
      case RenameKey:
        logRequest("RenameKey", omRequest);
        OzoneManagerProtocolProtos.RenameKeyRequest renameReq
            = (OzoneManagerProtocolProtos.RenameKeyRequest) omRequest.getRenameKeyRequest();

        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
            omRequest.getRenameKeyRequest().getKeyArgs(),
            new OperationArgs.RenameKeyArgs(renameReq.getToKeyName())));

        break;
      case DeleteKey:
        logRequest("DeleteKey", omRequest);
        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
            omRequest.getDeleteKeyRequest().getKeyArgs(),
            new OperationArgs.DeleteKeyArgs()));
        break;
      case CommitKey:
        logRequest("CommitKey", omRequest);
        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
            omRequest.getCommitKeyRequest().getKeyArgs(),
            new OperationArgs.CommitKeyArgs()));
        break;
      case CreateDirectory:
        logRequest("CreateDirectory", omRequest);
        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
            omRequest.getCreateDirectoryRequest().getKeyArgs(),
            new OperationArgs.CreateDirectoryArgs()));
        break;
      case CreateFile:
        logRequest("CreateFile", omRequest);

        OzoneManagerProtocolProtos.CreateFileRequest createFileReq
            = (OzoneManagerProtocolProtos.CreateFileRequest) omRequest.getCreateFileRequest();

        storeCompletedRequestInfo(buildOmCompletedRequestInfo(trxLogIndex,
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

  private OmCompletedRequestInfo buildOmCompletedRequestInfo(long trxLogIndex,
                                                             OzoneManagerProtocolProtos.KeyArgs keyArgs,
                                                             OperationArgs opArgs) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxLogIndex)
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setCreationTime(System.currentTimeMillis())
        .setOpArgs(opArgs)
        .build();
  }

  private void storeCompletedRequestInfo(OmCompletedRequestInfo requestInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Storing request info {}", requestInfo);
    }

    // XXX: not sure if this string key is necessary.  I added it as an
    // identifier which consumers of the ledger could use an efficient
    // (lexiographically sortable) key which could serve as a "seek
    // position" to continue reading where they left off (and to
    // persist to remember where they needed to carry on from).  But
    // that may be unnecessary.  TODO: can we just use a plain integer
    // key?
    String key = requestInfo.getDbKey();

    // XXX: should this be part of an atomic db txn that happens at the end
    // of each replayed event or batch of events so that the completed
    // request info "ledger" table is consistent with the processed
    // raits events?  e.g. OzoneManagerDoubleBuffer?

    try (BatchOperation batchOperation = omMetadataManager.getStore()
        .initBatchOperation()) {

      omMetadataManager.getCompletedRequestInfoTable().putWithBatch(batchOperation, key, requestInfo);

      // TODO: cap the size of the table to some configured limit.
      //
      // The following code is taken from
      // https://github.com/apache/ozone/pull/8779/files#r2510853726
      //
      // ... as a suggested approach but I think it will need amended to
      // work here because the code seems to be predicated on all txnids
      // being held (and therefore we can count the nuber of IDs to
      // delete by subtracting new txnid from the first) whereas
      // CompletedRequestInfoTable only holds the details of a subset of
      // "interesting" write requests and therefore there are gaps in
      // the IDs.
      //
      // I'm not sure how best to approach this.  A couple of( strawman)
      // ideas:
      //
      // 1. we could store every operation wherther interesting or not (or we
      // add dummy rows for the "non interesting" requests).
      // 2. we store an in memory count of the CompletedRequestInfoTable
      // which is initialized on startup and updated as rows are
      // added/cycled out. Therefore we know how many to cap the table
      // size as.
      //
      // TODO: revisit this
      //
      //omMetadataManager.getCompletedRequestInfoTable().deleteRangeWithBatch(batchOperation, 0L,
      //    Math.max(lastTransaction.getIndex() - maxFlushedTransactionGap, 0L));

      omMetadataManager.getStore().commitBatchOperation(batchOperation);

    //} catch (IOException ex) {
    //  LOG.error("Unable to write operation {}", requestInfo, ex);
    } catch (Exception ex) {
      LOG.error("Unable to write operation {}", requestInfo, ex);
    }
  }
}
