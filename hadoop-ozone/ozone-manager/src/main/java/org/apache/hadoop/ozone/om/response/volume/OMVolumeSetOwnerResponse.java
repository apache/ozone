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

package org.apache.hadoop.ozone.om.response.volume;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;

/**
 * Response for set owner request.
 */
@CleanupTableInfo(cleanupTables = {VOLUME_TABLE})
public class OMVolumeSetOwnerResponse extends OMClientResponse {
  private String oldOwner;
  private PersistedUserVolumeInfo oldOwnerVolumeList;
  private PersistedUserVolumeInfo newOwnerVolumeList;
  private OmVolumeArgs newOwnerVolumeArgs;

  public OMVolumeSetOwnerResponse(@Nonnull OMResponse omResponse,
      @Nonnull String oldOwner,
      @Nonnull PersistedUserVolumeInfo oldOwnerVolumeList,
      @Nonnull PersistedUserVolumeInfo newOwnerVolumeList,
      @Nonnull OmVolumeArgs newOwnerVolumeArgs) {
    super(omResponse);
    this.oldOwner = oldOwner;
    this.oldOwnerVolumeList = oldOwnerVolumeList;
    this.newOwnerVolumeList = newOwnerVolumeList;
    this.newOwnerVolumeArgs = newOwnerVolumeArgs;
  }

  /**
   * For when the request is not successful or when newOwner is the same as
   * oldOwner.
   * For other successful requests, the other constructor should be used.
   */
  public OMVolumeSetOwnerResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    // When newOwner is the same as oldOwner, status is OK but success is false.
    // We want to bypass the check in this case.
    if (omResponse.getSuccess()) {
      checkStatusNotOK();
    }
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // When newOwner is the same as oldOwner, status is OK but success is false.
    // We don't want to add it to DB batch in this case.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK &&
        getOMResponse().getSuccess()) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String oldOwnerKey = omMetadataManager.getUserKey(oldOwner);
    String newOwnerKey =
        omMetadataManager.getUserKey(newOwnerVolumeArgs.getOwnerName());
    if (oldOwnerVolumeList.getVolumeNamesList().isEmpty()) {
      omMetadataManager.getUserTable().deleteWithBatch(batchOperation,
          oldOwnerKey);
    } else {
      omMetadataManager.getUserTable().putWithBatch(batchOperation,
          oldOwnerKey, oldOwnerVolumeList);
    }
    omMetadataManager.getUserTable().putWithBatch(batchOperation, newOwnerKey,
        newOwnerVolumeList);

    String dbVolumeKey =
        omMetadataManager.getVolumeKey(newOwnerVolumeArgs.getVolume());
    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        dbVolumeKey, newOwnerVolumeArgs);
  }
}
