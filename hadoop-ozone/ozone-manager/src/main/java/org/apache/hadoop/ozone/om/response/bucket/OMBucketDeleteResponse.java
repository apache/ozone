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

package org.apache.hadoop.ozone.om.response.bucket;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;

/**
 * Response for DeleteBucket request.
 */
@CleanupTableInfo(cleanupTables = {BUCKET_TABLE})
public final class OMBucketDeleteResponse extends OMClientResponse {

  private String volumeName;
  private String bucketName;
  private final OmVolumeArgs omVolumeArgs;

  public OMBucketDeleteResponse(@Nonnull OMResponse omResponse,
      String volumeName, String bucketName, OmVolumeArgs volumeArgs) {
    super(omResponse);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.omVolumeArgs = volumeArgs;
  }

  public OMBucketDeleteResponse(@Nonnull OMResponse omResponse,
      String volumeName, String bucketName) {
    super(omResponse);
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.omVolumeArgs = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMBucketDeleteResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    this.omVolumeArgs = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbBucketKey =
        omMetadataManager.getBucketKey(volumeName, bucketName);
    omMetadataManager.getBucketTable().deleteWithBatch(batchOperation,
        dbBucketKey);
    // update volume usedNamespace
    if (omVolumeArgs != null) {
      omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
              omMetadataManager.getVolumeKey(omVolumeArgs.getVolume()),
              omVolumeArgs);
    }
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

}

