/*
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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;

/**
 * Response for {@link OMKeyPurgeRequest} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE, BUCKET_TABLE})
public class OMKeyPurgeResponse extends OmKeyResponse {
  private List<Pair<OmBucketInfo, List<String>>> purgeKeyList;

  public OMKeyPurgeResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<Pair<OmBucketInfo, List<String>>> bucketKeyList) {
    super(omResponse);
    this.purgeKeyList = bucketKeyList;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    for (Pair<OmBucketInfo, List<String>> bucketKeyListInfo : purgeKeyList) {
      for (String key : bucketKeyListInfo.getValue()) {
        omMetadataManager.getDeletedTable().deleteWithBatch(batchOperation,
            key);
      }
      // update bucket usedBytes.
      OmBucketInfo omBucketInfo = bucketKeyListInfo.getKey();
      omMetadataManager.getBucketTable().putWithBatch(batchOperation,
          omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
              omBucketInfo.getBucketName()), omBucketInfo);
    }
  }

}
