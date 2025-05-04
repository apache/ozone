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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response for CreateKey request.
 */
@CleanupTableInfo(cleanupTables = {OPEN_KEY_TABLE, KEY_TABLE, BUCKET_TABLE})
public class OMKeyCreateResponse extends OmKeyResponse {

  protected static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateResponse.class);
  private OmKeyInfo omKeyInfo;
  private long openKeySessionID;
  private List<OmKeyInfo> parentKeyInfos;
  private OmBucketInfo omBucketInfo;

  public OMKeyCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo omKeyInfo, List<OmKeyInfo> parentKeyInfos,
      long openKeySessionID, @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.omKeyInfo = omKeyInfo;
    this.openKeySessionID = openKeySessionID;
    this.parentKeyInfos = parentKeyInfos;
    this.omBucketInfo = omBucketInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyCreateResponse(@Nonnull OMResponse omResponse, @Nonnull
                             BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
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
        omMetadataManager.getKeyTable(getBucketLayout())
            .putWithBatch(batchOperation, parentKey, parentKeyInfo);
      }
      
      // namespace quota changes for parent directory
      String bucketKey = omMetadataManager.getBucketKey(
          getOmBucketInfo().getVolumeName(),
          getOmBucketInfo().getBucketName());
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          bucketKey, getOmBucketInfo());
    }

    String openKey = omMetadataManager.getOpenKey(omKeyInfo.getVolumeName(),
        omKeyInfo.getBucketName(), omKeyInfo.getKeyName(), openKeySessionID);
    omMetadataManager.getOpenKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, openKey, omKeyInfo);
  }

  protected long getOpenKeySessionID() {
    return openKeySessionID;
  }

  protected OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }

  protected OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }
}

