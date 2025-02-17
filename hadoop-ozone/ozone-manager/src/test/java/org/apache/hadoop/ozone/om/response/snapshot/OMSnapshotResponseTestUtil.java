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

package org.apache.hadoop.ozone.om.response.snapshot;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

/**
 * Common test utility method(s) shared between
 * {@link TestOMSnapshotCreateResponse} and
 * {@link TestOMSnapshotDeleteResponse}.
 */
public final class OMSnapshotResponseTestUtil {

  private OMSnapshotResponseTestUtil() {
    throw new IllegalStateException("Util class should not be initialized.");
  }

  static void addVolumeBucketInfoToTable(OMMetadataManager omMetadataManager,
                                         String volumeName,
                                         String bucketName)
      throws IOException {

    // Add volume entry to volumeTable for volumeId lookup
    final OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    final String dbVolumeKey = omMetadataManager.getVolumeKey(volumeName);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(dbVolumeKey),
        CacheValue.get(1L, omVolumeArgs));
    omMetadataManager.getVolumeTable().put(dbVolumeKey, omVolumeArgs);

    // Add bucket entry to bucketTable for bucketId lookup
    final OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    final String dbBucketKey = omMetadataManager.getBucketKey(
        volumeName, bucketName);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(dbBucketKey),
        CacheValue.get(2L, omBucketInfo));
    omMetadataManager.getBucketTable().put(dbBucketKey, omBucketInfo);
  }
}
