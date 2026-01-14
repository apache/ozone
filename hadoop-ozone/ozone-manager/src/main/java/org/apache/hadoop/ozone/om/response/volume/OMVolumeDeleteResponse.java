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
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.HasCompletedRequestInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;

/**
 * Response for DeleteVolume request.
 */
@CleanupTableInfo(cleanupTables = {VOLUME_TABLE})
public class OMVolumeDeleteResponse extends OMClientResponse implements HasCompletedRequestInfo {
  private String volume;
  private String owner;
  private PersistedUserVolumeInfo updatedVolumeList;

  public OMVolumeDeleteResponse(@Nonnull OMResponse omResponse,
      @Nonnull String volume, @Nonnull String owner,
      @Nonnull PersistedUserVolumeInfo updatedVolumeList) {
    super(omResponse);
    this.volume = volume;
    this.owner = owner;
    this.updatedVolumeList = updatedVolumeList;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMVolumeDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbUserKey = omMetadataManager.getUserKey(owner);
    PersistedUserVolumeInfo volumeList = updatedVolumeList;
    if (updatedVolumeList.getVolumeNamesList().isEmpty()) {
      omMetadataManager.getUserTable().deleteWithBatch(batchOperation,
          dbUserKey);
    } else {
      omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
          volumeList);
    }
    omMetadataManager.getVolumeTable().deleteWithBatch(batchOperation,
        omMetadataManager.getVolumeKey(volume));
  }

  @Override
  public OmCompletedRequestInfo getCompletedRequestInfo(long trxnLogIndex) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxnLogIndex)
        .setCmdType(Type.DeleteVolume)
        .setCreationTime(System.currentTimeMillis())
        .setVolumeName(volume)
        .setOpArgs(new OmCompletedRequestInfo.OperationArgs.NoArgs())
        .build();
  }
}
