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

package org.apache.hadoop.ozone.om.response.volume;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.storage.proto.
    OzoneManagerStorageProtos.PersistedUserVolumeInfo;

import org.apache.hadoop.hdds.utils.db.BatchOperation;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;

/**
 * Response for CreateVolume request.
 */
@CleanupTableInfo(cleanupTables = VOLUME_TABLE)
public class OMVolumeCreateResponse extends OMClientResponse {

  private PersistedUserVolumeInfo userVolumeInfo;
  private OmVolumeArgs omVolumeArgs;

  public OMVolumeCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmVolumeArgs omVolumeArgs,
      @Nonnull PersistedUserVolumeInfo userVolumeInfo) {
    super(omResponse);
    this.omVolumeArgs = omVolumeArgs;
    this.userVolumeInfo = userVolumeInfo;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMVolumeCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
  }

  @Override
  protected void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbVolumeKey =
        omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
    String dbUserKey =
        omMetadataManager.getUserKey(omVolumeArgs.getOwnerName());

    omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
        dbVolumeKey, omVolumeArgs);
    omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
        userVolumeInfo);
  }

  @VisibleForTesting
  public OmVolumeArgs getOmVolumeArgs() {
    return omVolumeArgs;
  }

}

