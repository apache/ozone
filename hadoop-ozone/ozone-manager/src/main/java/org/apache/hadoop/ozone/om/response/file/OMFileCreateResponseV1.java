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

package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

/**
 * Response for create file request layout version V1.
 */
@CleanupTableInfo(cleanupTables = {DIRECTORY_TABLE, OPEN_FILE_TABLE})
public class OMFileCreateResponseV1 extends OMFileCreateResponse {

  private List<OmDirectoryInfo> parentDirInfos;

  public OMFileCreateResponseV1(@Nonnull OMResponse omResponse,
                                @Nonnull OmKeyInfo omKeyInfo,
                                @Nonnull List<OmDirectoryInfo> parentDirInfos,
                                long openKeySessionID,
                                @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omKeyInfo, new ArrayList<>(), openKeySessionID,
        omBucketInfo);
    this.parentDirInfos = parentDirInfos;
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
        String parentKey = parentDirInfo.getPath();
        if (LOG.isDebugEnabled()) {
          LOG.debug("putWithBatch adding parent : key {} info : {}", parentKey,
                  parentDirInfo);
        }
        omMetadataMgr.getDirectoryTable().putWithBatch(batchOp, parentKey,
                parentDirInfo);
      }
    }

    OMFileRequest.addToOpenFileTable(omMetadataMgr, batchOp, getOmKeyInfo(),
            getOpenKeySessionID());

    // update bucket usedBytes.
    omMetadataMgr.getBucketTable().putWithBatch(batchOp,
            omMetadataMgr.getBucketKey(getOmKeyInfo().getVolumeName(),
                    getOmKeyInfo().getBucketName()), getOmBucketInfo());
  }

}
