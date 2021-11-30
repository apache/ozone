/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request;

import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeysRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMPathsPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;

import java.io.IOException;

/**
 * OM Key factory to create key, file om requests.
 */
public final class OMKeyRequestFactory {

  private OMKeyRequestFactory() {
  }

  /**
   * Create OM request based on the bucket layout type.
   *
   * @param omRequest    om key request
   * @param ozoneManager ozone manager
   * @return omKeyRequest
   * @throws IOException
   */
  public static OMKeyRequest createRequest(OMRequest omRequest,
      OzoneManager ozoneManager) throws IOException {

    Type cmdType = omRequest.getCmdType();
    KeyArgs keyArgs;
    OMKeyRequest omKeyRequest = null;

    switch (cmdType) {
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      omKeyRequest = OMAllocateBlockRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      omKeyRequest =
          OMKeyCreateRequest.getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      omKeyRequest =
          OMKeyCommitRequest.getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      omKeyRequest =
          OMKeyDeleteRequest.getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case DeleteKeys:
      return new OMKeysDeleteRequest(omRequest);
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      omKeyRequest =
          OMKeyRenameRequest.getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case RenameKeys:
      return new OMKeysRenameRequest(omRequest);
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      omKeyRequest = OMDirectoryCreateRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      omKeyRequest =
          OMFileCreateRequest.getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case PurgeKeys:
      omKeyRequest = new OMKeyPurgeRequest(omRequest);
      break;
    case PurgePaths:
      omKeyRequest = new OMPathsPurgeRequestWithFSO(omRequest);
      break;
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      omKeyRequest = S3InitiateMultipartUploadRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      omKeyRequest = S3MultipartUploadCommitPartRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      omKeyRequest = S3MultipartUploadAbortRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      omKeyRequest = S3MultipartUploadCompleteRequest
          .getInstance(keyArgs, omRequest, ozoneManager);
      break;
    default:
      // not required to handle here, its handled by the caller of
      // #createRequest() method.
      break;
    }
    return omKeyRequest;
  }
}
