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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VERSIONED_KEY_TABLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Response for a DeleteKey request on a bucket with S3-compatible versioning
 * enabled: no data is removed. A delete marker becomes the current version in
 * the keyTable, and the version it supersedes (if the key existed) moves to
 * the versionedKeyTable.
 */
@CleanupTableInfo(cleanupTables = {KEY_TABLE, VERSIONED_KEY_TABLE, BUCKET_TABLE,
    DELETED_TABLE})
public class OMKeyDeleteMarkerResponse extends OmKeyResponse {

  private OmKeyInfo deleteMarker;
  private String ozoneKeyName;
  private String movedVersionedKeyName;
  private OmKeyInfo movedVersionedKeyInfo;
  private OmBucketInfo omBucketInfo;
  private String replacedNullVersionKey;
  private Map<String, RepeatedOmKeyInfo> keysToDelete;

  /**
   * @param movedVersionedKeyName the versionedKeyTable entry the superseded
   *     version moves to, or null when the marker replaced it instead
   * @param replacedNullVersionKey the versionedKeyTable entry of the null
   *     version the marker replaced, when versioning is suspended and it had
   *     one
   * @param keysToDelete the replaced null version's blocks, queued for
   *     reclamation
   */
  @SuppressWarnings("parameternumber")
  public OMKeyDeleteMarkerResponse(@Nonnull OMResponse omResponse,
      @Nonnull OmKeyInfo deleteMarker, @Nonnull String ozoneKeyName,
      String movedVersionedKeyName, OmKeyInfo movedVersionedKeyInfo,
      @Nonnull OmBucketInfo omBucketInfo, String replacedNullVersionKey,
      Map<String, RepeatedOmKeyInfo> keysToDelete) {
    super(omResponse, omBucketInfo.getBucketLayout());
    this.deleteMarker = deleteMarker;
    this.ozoneKeyName = ozoneKeyName;
    this.movedVersionedKeyName = movedVersionedKeyName;
    this.movedVersionedKeyInfo = movedVersionedKeyInfo;
    this.omBucketInfo = omBucketInfo;
    this.replacedNullVersionKey = replacedNullVersionKey;
    this.keysToDelete = keysToDelete;
  }

  /**
   * For when the request is not successful.
   * For a successful request, the other constructor should be used.
   */
  public OMKeyDeleteMarkerResponse(@Nonnull OMResponse omResponse,
      @Nonnull BucketLayout bucketLayout) {
    super(omResponse, bucketLayout);
    checkStatusNotOK();
  }

  @VisibleForTesting
  public Map<String, RepeatedOmKeyInfo> getKeysToDelete() {
    return keysToDelete;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    omMetadataManager.getKeyTable(getBucketLayout())
        .putWithBatch(batchOperation, ozoneKeyName, deleteMarker);

    if (movedVersionedKeyInfo != null) {
      omMetadataManager.getVersionedKeyTable().putWithBatch(batchOperation,
          movedVersionedKeyName, movedVersionedKeyInfo);
    }

    if (replacedNullVersionKey != null) {
      omMetadataManager.getVersionedKeyTable()
          .deleteWithBatch(batchOperation, replacedNullVersionKey);
    }

    if (keysToDelete != null) {
      for (Map.Entry<String, RepeatedOmKeyInfo> entry
          : keysToDelete.entrySet()) {
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            entry.getKey(), entry.getValue());
      }
    }

    omMetadataManager.getBucketTable().putWithBatch(batchOperation,
        omMetadataManager.getBucketKey(omBucketInfo.getVolumeName(),
            omBucketInfo.getBucketName()), omBucketInfo);
  }
}
