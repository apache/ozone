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

package org.apache.hadoop.ozone.om.ratis.execution.factory;

import java.io.IOException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMPersistDbRequest;
import org.apache.hadoop.ozone.om.ratis.execution.request.OMRequestBase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class used by OzoneManager HA.
 */
public final class OmRequestFactory {
  private static final Logger LOG = LoggerFactory.getLogger(OmRequestFactory.class);

  private OmRequestFactory() {
  }

  /**
   * Create OMClientRequest which encapsulates the OMRequest.
   * @param omRequest
   * @return OMClientRequest
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:methodlength")
  public static OMRequestBase createClientRequest(
      OMRequest omRequest, OzoneManager ozoneManager) throws IOException {
    // Handling of exception by createClientRequest(OMRequest, OzoneManger):
    // Either the code will take FSO or non FSO path, both classes has a
    // validateAndUpdateCache() function which also contains
    // validateBucketAndVolume() function which validates bucket and volume and
    // throws necessary exceptions if required. validateAndUpdateCache()
    // function has catch block which catches the exception if required and
    // handles it appropriately.
    Type cmdType = omRequest.getCmdType();
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    String volumeName = "";
    String bucketName = "";

    switch (cmdType) {
    case PersistDb:
      return new OMPersistDbRequest(omRequest);
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    default:
      throw new OMException("Unrecognized write command type request "
          + cmdType, OMException.ResultCodes.INVALID_REQUEST);
    }

    return BucketLayoutAwareOMKeyRequestFactory.createRequest(
        volumeName, bucketName, omRequest, ozoneManager.getMetadataManager());
  }
}
