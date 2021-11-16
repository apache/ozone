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

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMPathsPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
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
 * Response for {@link OMPathsPurgeRequestWithFSO} request.
 */
@CleanupTableInfo(cleanupTables = {DELETED_TABLE, DELETED_DIR_TABLE,
    DIRECTORY_TABLE, FILE_TABLE})
public class OMPathsPurgeResponseWithFSO extends OMClientResponse {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMPathsPurgeResponseWithFSO.class);

  private List<OzoneManagerProtocolProtos.KeyInfo> markDeletedDirList;
  private List<String> dirList;
  private List<OzoneManagerProtocolProtos.KeyInfo> fileList;
  private boolean isRatisEnabled;


  public OMPathsPurgeResponseWithFSO(@Nonnull OMResponse omResponse,
      @Nonnull List<OzoneManagerProtocolProtos.KeyInfo> markDeletedDirs,
      @Nonnull List<OzoneManagerProtocolProtos.KeyInfo> files,
      @Nonnull List<String> dirs, boolean isRatisEnabled) {
    super(omResponse);
    this.markDeletedDirList = markDeletedDirs;
    this.dirList = dirs;
    this.fileList = files;
    this.isRatisEnabled = isRatisEnabled;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // Add all sub-directories to deleted directory table.
    for (OzoneManagerProtocolProtos.KeyInfo key : markDeletedDirList) {
      OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
      String ozoneDbKey = omMetadataManager.getOzonePathKey(
          keyInfo.getParentObjectID(), keyInfo.getFileName());
      omMetadataManager.getDeletedDirTable().putWithBatch(batchOperation,
          ozoneDbKey, keyInfo);

      omMetadataManager.getDirectoryTable().deleteWithBatch(batchOperation,
          ozoneDbKey);

      if (LOG.isDebugEnabled()) {
        LOG.debug("markDeletedDirList KeyName: {}, DBKey: {}",
            keyInfo.getKeyName(), ozoneDbKey);
      }
    }

    // Delete all the visited directories from deleted directory table
    for (String key : dirList) {
      omMetadataManager.getDeletedDirTable().deleteWithBatch(batchOperation,
          key);

      if (LOG.isDebugEnabled()) {
        LOG.info("Purge Deleted Directory DBKey: {}", key);
      }
    }
    for (OzoneManagerProtocolProtos.KeyInfo key : fileList) {
      OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
      String ozoneDbKey = omMetadataManager.getOzonePathKey(
          keyInfo.getParentObjectID(), keyInfo.getFileName());
      omMetadataManager.getKeyTable(getBucketLayout())
          .deleteWithBatch(batchOperation, ozoneDbKey);

      if (LOG.isDebugEnabled()) {
        LOG.info("Move keyName:{} to DeletedTable DBKey: {}",
            keyInfo.getKeyName(), ozoneDbKey);
      }

      RepeatedOmKeyInfo repeatedOmKeyInfo = null;
      repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(keyInfo,
          repeatedOmKeyInfo, keyInfo.getUpdateID(), isRatisEnabled);

      String deletedKey = omMetadataManager
          .getOzoneKey(keyInfo.getVolumeName(), keyInfo.getBucketName(),
              keyInfo.getKeyName());

      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          deletedKey, repeatedOmKeyInfo);

    }
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
