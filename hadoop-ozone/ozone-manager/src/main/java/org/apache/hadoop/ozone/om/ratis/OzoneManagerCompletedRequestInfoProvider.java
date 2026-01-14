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

package org.apache.hadoop.ozone.om.ratis;

import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo.OperationArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class when given a request will determine if the request is one of
 * a subset of write requests which we want to save to the
 * OmCompletedRequestInfo ledger and if so it will return the
 * appropriate instance.
 *
 * NOTE: as per elsewhere - this class may not need to exist if we go
 * with the approach of the subset of response handlers which we want to
 * populate a OmCompletedRequestInfo ledger row can do so themselves
 * (e.g. in OMKeyCommitResponse::addToBatch)
 */
public final class OzoneManagerCompletedRequestInfoProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerCompletedRequestInfoProvider.class);

  public OmCompletedRequestInfo get(long trxLogIndex, OzoneManagerProtocolProtos.OMRequest omRequest) {

    switch (omRequest.getCmdType()) {
    case CreateVolume:
      logRequest("CreateVolume", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), null)
          .setVolumeName(omRequest.getCreateVolumeRequest().getVolumeInfo().getVolume())
          .setOpArgs(new OperationArgs.CreateVolumeArgs())
          .build();

    case DeleteVolume:
      logRequest("DeleteVolume", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), null)
          .setVolumeName(omRequest.getDeleteVolumeRequest().getVolumeName())
          .setOpArgs(new OperationArgs.DeleteVolumeArgs())
          .build();

    case CreateBucket:
      logRequest("CreateBucket", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), null)
          .setVolumeName(omRequest.getCreateBucketRequest().getBucketInfo().getVolumeName())
          .setBucketName(omRequest.getCreateBucketRequest().getBucketInfo().getBucketName())
          .setOpArgs(new OperationArgs.CreateBucketArgs())
          .build();

    case DeleteBucket:
      logRequest("DeleteBucket", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), null)
          .setVolumeName(omRequest.getDeleteBucketRequest().getVolumeName())
          .setBucketName(omRequest.getDeleteBucketRequest().getBucketName())
          .setOpArgs(new OperationArgs.DeleteBucketArgs())
          .build();

    case CreateKey:
      logRequest("CreateKey", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getCreateKeyRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.CreateKeyArgs())
          .build();

    case RenameKey:
      logRequest("RenameKey", omRequest);
      OzoneManagerProtocolProtos.RenameKeyRequest renameReq
          = (OzoneManagerProtocolProtos.RenameKeyRequest) omRequest.getRenameKeyRequest();

      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getRenameKeyRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.RenameKeyArgs(renameReq.getToKeyName()))
          .build();

    case DeleteKey:
      logRequest("DeleteKey", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getDeleteKeyRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.DeleteKeyArgs())
          .build();

    case CommitKey:
      logRequest("CommitKey", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getCommitKeyRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.CommitKeyArgs())
          .build();

    case CreateDirectory:
      logRequest("CreateDirectory", omRequest);
      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getCreateDirectoryRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.CreateDirectoryArgs())
          .build();

    case CreateFile:
      logRequest("CreateFile", omRequest);

      OzoneManagerProtocolProtos.CreateFileRequest createFileReq
          = (OzoneManagerProtocolProtos.CreateFileRequest) omRequest.getCreateFileRequest();

      return requestInfoBuilder(trxLogIndex, omRequest.getCmdType(), omRequest.getCreateFileRequest().getKeyArgs())
          .setOpArgs(new OperationArgs.CreateFileArgs(createFileReq.getIsRecursive(),
                                                      createFileReq.getIsOverwrite()))
          .build();

    default:
      LOG.debug("Unhandled cmdType={}", omRequest.getCmdType());
      return null;
    }
  }

  private static void logRequest(String label, OzoneManagerProtocolProtos.OMRequest omRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("---> {} {}", label, omRequest);
    }
  }

  private OmCompletedRequestInfo.Builder requestInfoBuilder(long trxLogIndex,
                                                            OzoneManagerProtocolProtos.Type cmdType,
                                                            OzoneManagerProtocolProtos.KeyArgs keyArgs) {

    OmCompletedRequestInfo.Builder builder = OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxLogIndex)
        .setCmdType(cmdType)
        .setCreationTime(System.currentTimeMillis());

    if (keyArgs != null) {
      builder.setVolumeName(keyArgs.getVolumeName());
      builder.setBucketName(keyArgs.getBucketName());
      builder.setKeyName(keyArgs.getKeyName());
    }

    return builder;
  }
}
