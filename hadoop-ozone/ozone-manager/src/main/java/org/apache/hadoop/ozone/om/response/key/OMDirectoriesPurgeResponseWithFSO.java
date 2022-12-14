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

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMDirectoriesPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;

/**
 * Response for {@link OMDirectoriesPurgeRequestWithFSO} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE, DELETED_DIR_TABLE,
    DIRECTORY_TABLE, FILE_TABLE})
public class OMDirectoriesPurgeResponseWithFSO extends OmKeyResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoriesPurgeResponseWithFSO.class);

  private List<OzoneManagerProtocolProtos.PurgePathRequest> paths;
  private boolean isRatisEnabled;
  private Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap;


  public OMDirectoriesPurgeResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull List<OzoneManagerProtocolProtos.PurgePathRequest> paths,
      boolean isRatisEnabled, @Nonnull BucketLayout bucketLayout,
      Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap) {
    super(omResponse, bucketLayout);
    this.paths = paths;
    this.isRatisEnabled = isRatisEnabled;
    this.volBucketInfoMap = volBucketInfoMap;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    for (OzoneManagerProtocolProtos.PurgePathRequest path : paths) {
      final long volumeId = path.getVolumeId();
      final long bucketId = path.getBucketId();

      final List<OzoneManagerProtocolProtos.KeyInfo> deletedSubFilesList =
              path.getDeletedSubFilesList();
      final List<OzoneManagerProtocolProtos.KeyInfo> markDeletedSubDirsList =
              path.getMarkDeletedSubDirsList();

      // Add all sub-directories to deleted directory table.
      for (OzoneManagerProtocolProtos.KeyInfo key : markDeletedSubDirsList) {
        OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
        String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId,
                bucketId, keyInfo.getParentObjectID(), keyInfo.getFileName());
        omMetadataManager.getDeletedDirTable().putWithBatch(batchOperation,
                ozoneDbKey, keyInfo);

        omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
                ozoneDbKey);

        if (LOG.isDebugEnabled()) {
          LOG.debug("markDeletedDirList KeyName: {}, DBKey: {}",
                  keyInfo.getKeyName(), ozoneDbKey);
        }
      }

      for (OzoneManagerProtocolProtos.KeyInfo key : deletedSubFilesList) {
        OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
        String ozoneDbKey = omMetadataManager.getOzonePathKey(volumeId,
                bucketId, keyInfo.getParentObjectID(), keyInfo.getFileName());
        omMetadataManager.getKeyTable(getBucketLayout())
                .deleteWithBatch(batchOperation, ozoneDbKey);

        if (LOG.isDebugEnabled()) {
          LOG.info("Move keyName:{} to DeletedTable DBKey: {}",
                  keyInfo.getKeyName(), ozoneDbKey);
        }

        RepeatedOmKeyInfo repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
                keyInfo, null, keyInfo.getUpdateID(), isRatisEnabled);

        String deletedKey = omMetadataManager
                .getOzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
                        keyInfo.getKeyName());

        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
                deletedKey, repeatedOmKeyInfo);

      }

      // Delete the visited directory from deleted directory table
      if (path.hasDeletedDir()) {
        omMetadataManager.getDeletedDirTable().deleteWithBatch(batchOperation,
                path.getDeletedDir());

        if (LOG.isDebugEnabled()) {
          LOG.info("Purge Deleted Directory DBKey: {}", path.getDeletedDir());
        }
      }

      // update bucket usedBytes.
      for (OmBucketInfo omBucketInfo : volBucketInfoMap.values()) {
        omMetadataManager.getBucketTable().putWithBatch(batchOperation,
            omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
                omBucketInfo.getBucketName()), omBucketInfo);
      }
    }
  }
}
