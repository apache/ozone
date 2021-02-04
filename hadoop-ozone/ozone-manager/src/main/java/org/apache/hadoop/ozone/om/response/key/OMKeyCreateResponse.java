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

package org.apache.hadoop.ozone.om.response.key;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_KEY_TABLE;

/**
 * Response for CreateKey request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE})
public class OMKeyCreateResponse extends OMClientResponse {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateResponse.class);
  private OmKeyInfo omKeyInfo;
  private long openKeySessionID;
  private List<OmKeyInfo> parentKeyInfos;
  private OmBucketInfo omBucketInfo;

  public OMKeyCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, List<OmKeyInfo> parentKeyInfos,
      long openKeySessionID, @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
    this.openKeySessionID = openKeySessionID;
    this.parentKeyInfos = parentKeyInfos;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    /**
     * Create parent directory entries during Key Create - do not wait
     * for Key Commit request.
     * XXX handle stale directory entries.
     */
    if (parentKeyInfos != null) {
      for (OmKeyInfo parentKeyInfo : parentKeyInfos) {
        String parentKey = omMetadataManager
            .getOzoneDirKey(parentKeyInfo.getVolumeName(),
                parentKeyInfo.getBucketName(), parentKeyInfo.getKeyName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("putWithBatch adding parent : key {} info : {}", parentKey,
              parentKeyInfo);
        }
        omMetadataManager.getKeyTable()
            .putWithBatch(batchOperation, parentKey, parentKeyInfo);
      }
    }

    String openKey = omMetadataManager.getOpenKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName(), openKeySessionID);
    omMetadataManager.getOpenKeyTable().putWithBatch(batchOperation,
        openKey, omKeyInfo);

    // update bucket usedBytes.
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName()), omBucketInfo);
  }
}

