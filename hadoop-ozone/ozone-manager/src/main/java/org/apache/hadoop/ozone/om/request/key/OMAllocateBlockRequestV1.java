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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMAllocateBlockResponseV1;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Handles allocate block request layout version V1.
 */
public class OMAllocateBlockRequestV1 extends OMAllocateBlockRequest {

  private static final Logger LOG =
          LoggerFactory.getLogger(OMAllocateBlockRequestV1.class);

  public OMAllocateBlockRequestV1(OMRequest omRequest) {
    super(omRequest);
  }

  protected OmKeyInfo getOpenKeyInfo(OMMetadataManager omMetadataManager,
      String openKeyName, String keyName) throws IOException {
    String fileName = OzoneFSUtils.getFileName(keyName);
    return OMFileRequest.getOmKeyInfoFromFileTable(true,
            omMetadataManager, openKeyName, fileName);
  }

  protected String getOpenKeyName(String volumeName, String bucketName,
                                  String keyName, long clientID, OzoneManager ozoneManager)
          throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketId = omBucketInfo.getObjectID();
    String fileName = OzoneFSUtils.getFileName(keyName);
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    long parentID = OMFileRequest.getParentID(bucketId, pathComponents,
            keyName, ozoneManager);
    return omMetadataManager.getOpenFileName(parentID, fileName,
            clientID);
  }

  protected void addOpenTableCacheEntry(long trxnLogIndex,
      OMMetadataManager omMetadataManager, String openKeyName,
      OmKeyInfo openKeyInfo) {
    String fileName = openKeyInfo.getFileName();
    OMFileRequest.addOpenFileTableCacheEntry(omMetadataManager, openKeyName,
            openKeyInfo, fileName, trxnLogIndex);
  }

  @NotNull
  protected OMClientResponse getOmClientResponse(long clientID,
      OMResponse.Builder omResponse, OmKeyInfo openKeyInfo,
      OmVolumeArgs omVolumeArgs, OmBucketInfo omBucketInfo) {
    return new OMAllocateBlockResponseV1(omResponse.build(),
            openKeyInfo, clientID, omVolumeArgs, omBucketInfo);
  }
}