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

package org.apache.hadoop.ozone.om.response.file;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for create file request - prefix layout.
 */
@CleanupTableInfo(cleanupTables = {DIRECTORY_TABLE, OPEN_FILE_TABLE,
    BUCKET_TABLE})
public class OMFileCreateResponseWithFSO extends OMFileCreateResponse {

  private List<OmDirectoryInfo> parentDirInfos;
  private long volumeId;

  public OMFileCreateResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo,
      @Nonnull List<OmDirectoryInfo> parentDirInfos, long openKeySessionID,
      @Nonnull OmBucketInfo omBucketInfo, @Nonnull long volumeId) {
    super(omResponse, omKeyInfo, new ArrayList<>(), openKeySessionID,
        omBucketInfo);
    this.parentDirInfos = parentDirInfos;
    this.volumeId = volumeId;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMFileCreateResponseWithFSO(@Nonnull OMResponse omResponse,
                                     @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataMgr,
                              BatchOperation batchOp) throws IOException {

    /**
     * Create parent directory entries during Key Create - do not wait
     * for Key Commit request.
     * XXX handle stale directory entries.
     */
    if (parentDirInfos != null) {
      for (OmDirectoryInfo parentDirInfo : parentDirInfos) {
        String parentKey = omMetadataMgr.getOzonePathKey(volumeId,
            getOmBucketInfo().getObjectID(), parentDirInfo.getParentObjectID(),
            parentDirInfo.getName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("putWithBatch adding parent : key {} info : {}", parentKey,
                  parentDirInfo);
        }
        omMetadataMgr.getDirectoryTable().putWithBatch(batchOp, parentKey,
                parentDirInfo);
      }

      String bucketKey = omMetadataMgr.getBucketKey(
          getOmBucketInfo().getVolumeName(),
          getOmBucketInfo().getBucketName());
      omMetadataMgr.getBucketTable().putWithBatch(batchOp,
          bucketKey, getOmBucketInfo());
    }

    OMFileRequest.addToOpenFileTable(omMetadataMgr, batchOp, getOmKeyInfo(),
        getOpenKeySessionID(), volumeId, getOmBucketInfo().getObjectID());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
