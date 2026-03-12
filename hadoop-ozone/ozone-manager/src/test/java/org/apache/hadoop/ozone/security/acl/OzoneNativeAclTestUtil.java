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

package org.apache.hadoop.ozone.security.acl;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

/** Helper for ACL tests. */
final class OzoneNativeAclTestUtil {

  public static void addVolumeAcl(
      OMMetadataManager metadataManager,
      String volume,
      OzoneAcl ozoneAcl
  ) throws IOException {
    final String volumeKey = metadataManager.getVolumeKey(volume);
    final Table<String, OmVolumeArgs> volumeTable = metadataManager.getVolumeTable();
    final OmVolumeArgs omVolumeArgs = volumeTable.get(volumeKey)
        .toBuilder()
        .addAcl(ozoneAcl)
        .build();

    volumeTable.addCacheEntry(
        new CacheKey<>(volumeKey),
        CacheValue.get(1L, omVolumeArgs));
  }

  public static void addBucketAcl(
      OMMetadataManager metadataManager,
      String volume,
      String bucket,
      OzoneAcl ozoneAcl) throws IOException {
    final String bucketKey = metadataManager.getBucketKey(volume, bucket);
    final Table<String, OmBucketInfo> bucketTable = metadataManager.getBucketTable();
    final OmBucketInfo omBucketInfo = bucketTable.get(bucketKey)
        .toBuilder()
        .addAcl(ozoneAcl)
        .build();

    bucketTable.addCacheEntry(
        new CacheKey<>(bucketKey),
        CacheValue.get(1L, omBucketInfo));
  }

  public static void addKeyAcl(
      OMMetadataManager metadataManager,
      String volume,
      String bucket,
      BucketLayout bucketLayout,
      String key,
      OzoneAcl ozoneAcl
  ) throws IOException {
    final String objKey = metadataManager.getOzoneKey(volume, bucket, key);
    final Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(bucketLayout);
    final OmKeyInfo omKeyInfo = keyTable.get(objKey)
        .toBuilder()
        .addAcl(ozoneAcl)
        .build();

    keyTable.addCacheEntry(
        new CacheKey<>(objKey),
        CacheValue.get(1L, omKeyInfo));
  }

  public static void setVolumeAcl(
      OMMetadataManager metadataManager,
      String volume,
      List<OzoneAcl> ozoneAcls) throws IOException {
    final String volumeKey = metadataManager.getVolumeKey(volume);
    final Table<String, OmVolumeArgs> volumeTable = metadataManager.getVolumeTable();
    final OmVolumeArgs omVolumeArgs = volumeTable.get(volumeKey)
        .toBuilder()
        .setAcls(ozoneAcls)
        .build();

    volumeTable.addCacheEntry(
        new CacheKey<>(volumeKey),
        CacheValue.get(1L, omVolumeArgs));
  }

  public static void setBucketAcl(
      OMMetadataManager metadataManager,
      String volume,
      String bucket,
      List<OzoneAcl> ozoneAcls) throws IOException {
    final String bucketKey = metadataManager.getBucketKey(volume, bucket);
    final Table<String, OmBucketInfo> bucketTable = metadataManager.getBucketTable();
    final OmBucketInfo omBucketInfo = bucketTable.get(bucketKey)
        .toBuilder()
        .setAcls(ozoneAcls)
        .build();

    bucketTable.addCacheEntry(
        new CacheKey<>(bucketKey),
        CacheValue.get(1L, omBucketInfo));
  }

  public static void setKeyAcl(
      OMMetadataManager metadataManager,
      String volume,
      String bucket,
      BucketLayout bucketLayout,
      String key,
      List<OzoneAcl> ozoneAcls) throws IOException {
    final String objKey = metadataManager.getOzoneKey(volume, bucket, key);
    final Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(bucketLayout);
    final OmKeyInfo omKeyInfo = keyTable.get(objKey)
        .toBuilder()
        .setAcls(ozoneAcls)
        .build();

    keyTable.addCacheEntry(
        new CacheKey<>(objKey),
        CacheValue.get(1L, omKeyInfo));
  }

  public static List<OzoneAcl> getVolumeAcls(
      OMMetadataManager metadataManager,
      String volume
  ) throws IOException {
    return metadataManager.getVolumeTable()
        .get(metadataManager.getVolumeKey(volume))
        .getAcls();
  }

  public static List<OzoneAcl> getBucketAcls(
      OMMetadataManager metadataManager,
      String volume,
      String bucket
  ) throws IOException {
    return metadataManager.getBucketTable()
        .get(metadataManager.getBucketKey(volume, bucket))
        .getAcls();
  }

  public static List<OzoneAcl> getKeyAcls(
      OMMetadataManager metadataManager,
      String volume,
      String bucket,
      BucketLayout bucketLayout,
      String key
  ) throws IOException {
    return metadataManager.getKeyTable(bucketLayout)
        .get(metadataManager.getOzoneKey(volume, bucket, key))
        .getAcls();
  }

  private OzoneNativeAclTestUtil() {
    // utilities
  }
}
