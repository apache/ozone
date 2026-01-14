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

package org.apache.hadoop.ozone.om.response.bucket;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.HasCompletedRequestInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Response for CreateBucket request.
 */
@CleanupTableInfo(cleanupTables = {BUCKET_TABLE, VOLUME_TABLE})
public final class OMBucketCreateResponse extends OMClientResponse implements HasCompletedRequestInfo {

  private final OmBucketInfo omBucketInfo;
  private final OmVolumeArgs omVolumeArgs;

  public OMBucketCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmBucketInfo omBucketInfo, @Nonnull OmVolumeArgs omVolumeArgs) {
    super(omResponse);
    this.omBucketInfo = omBucketInfo;
    this.omVolumeArgs = omVolumeArgs;
  }

  public OMBucketCreateResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmBucketInfo omBucketInfo) {
    super(omResponse);
    this.omBucketInfo = omBucketInfo;
    this.omVolumeArgs = null;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMBucketCreateResponse(@Nonnull OMResponse omResponse) {
    super(omResponse);
    checkStatusNotOK();
    omBucketInfo = null;
    omVolumeArgs = null;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    String dbBucketKey =
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName());
    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        dbBucketKey, omBucketInfo);
    // update volume usedNamespace
    if (omVolumeArgs != null) {
      omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
              omMetadataManager.getVolumeKey(omVolumeArgs.getVolume()),
              omVolumeArgs);
    }
  }

  @Nullable
  public OmBucketInfo getOmBucketInfo() {
    return omBucketInfo;
  }

  @Override
  public OmCompletedRequestInfo getCompletedRequestInfo(long trxnLogIndex) {
    return OmCompletedRequestInfo.newBuilder()
        .setTrxLogIndex(trxnLogIndex)
        .setCmdType(Type.CreateBucket)
        .setCreationTime(System.currentTimeMillis())
        .setVolumeName(omBucketInfo.getVolumeName())
        .setBucketName(omBucketInfo.getBucketName())
        .setOpArgs(new OmCompletedRequestInfo.OperationArgs.NoArgs())
        .build();
  }
}

