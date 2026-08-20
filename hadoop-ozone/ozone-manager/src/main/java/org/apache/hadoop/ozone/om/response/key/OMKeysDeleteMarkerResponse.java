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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.LIFECYCLE_SCAN_STATE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VERSIONED_KEY_TABLE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.request.key.DeleteMarkerInsertion;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for a batch DeleteKeys request on a bucket with S3-compatible
 * versioning enabled: no data is removed. Each key gets a delete marker as its
 * current version, and the version each marker supersedes moves to the
 * versionedKeyTable.
 *
 * <p>The single-key counterpart is {@link OMKeyDeleteMarkerResponse}; both
 * write out what {@code OMKeyRequest.insertDeleteMarker} produced.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, VERSIONED_KEY_TABLE, BUCKET_TABLE,
    LIFECYCLE_SCAN_STATE_TABLE})
public class OMKeysDeleteMarkerResponse extends OmKeyResponse {

  private List<DeleteMarkerInsertion> insertions;
  private OmBucketInfo omBucketInfo;
  private OmLifecycleScanState scanState;

  public OMKeysDeleteMarkerResponse(@Nonnull OMResponse omResponse,
      @Nonnull List<DeleteMarkerInsertion> insertions,
      @Nonnull OmBucketInfo omBucketInfo, OmLifecycleScanState scanState) {
    super(omResponse, BucketLayout.OBJECT_STORE);
    this.insertions = insertions;
    this.omBucketInfo = omBucketInfo;
    this.scanState = scanState;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeysDeleteMarkerResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @Override
  public void checkAndUpdateDB(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    if (getOMResponse().getStatus() == OK
        || getOMResponse().getStatus() == PARTIAL_DELETE) {
      addToDBBatch(omMetadataManager, batchOperation);
    }
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    for (DeleteMarkerInsertion inserted : insertions) {
      // The version the marker supersedes and the marker itself go in one
      // batch, so a reader never sees the key without either.
      if (inserted.getDemotedVersion() != null) {
        omMetadataManager.getVersionedKeyTable().putWithBatch(batchOperation,
            inserted.getDemotedVersionKey(), inserted.getDemotedVersion());
      }
      omMetadataManager.getKeyTable(getBucketLayout()).putWithBatch(
          batchOperation, inserted.getObjectKey(), inserted.getDeleteMarker());
    }

    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);

    if (scanState != null) {
      omMetadataManager.getLifecycleScanStateTable().putWithBatch(
          batchOperation, scanState.getBucketKey(), scanState);
    }
  }
}
