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

package org.apache.hadoop.ozone.om.request.util;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.util.UUIDv7;

/**
 * Utility class related to OM Multipart Upload.
 */
public final class OMMultipartUploadUtils {

  private OMMultipartUploadUtils() {
  }

  /**
   * Generate a unique Multipart Upload ID.
   * @return multipart upload ID
   */
  public static String getMultipartUploadId() {
    return UUIDv7.randomUUID() + "-" + UniqueId.next();
  }

  /**
   * Get multipart upload ID from the DB key of multipart upload
   * form openKeyTable/openFileTable.
   *
   * The DB keys of openKeyTable and openFileTable are different:
   *   openKeyTable: /{volumeName}/{bucketName}/{keyName}/{uploadId}
   *   openFileTable: /{volumeId}/{bucketId}/{parentId}/{fileName}/{uploadId}
   *
   *
   * Despite the difference, both have the uploadId as the suffix of the DB
   * key, we can extract this suffix to get the upload ID from the DB key.
   *
   * Upload ID format: uploadId = UUIDv4 + "-" + UniqueId#next
   *
   * @param key DB key
   * @return upload ID if uploadId can be extracted from openDBKey
   *         otherwise null
   */
  public static String getUploadIdFromDbKey(String key) {
    String[] split = key.split(OM_KEY_PREFIX);
    if (split.length < 5) {
      return null;
    }

    String uploadId = split[split.length - 1];

    // Similar to the logic of UUID#fromString, but use 6 since there is
    // another "-" between the UUID and the UniqueId
    if (StringUtils.isEmpty(uploadId) ||
        uploadId.split("-").length != 6) {
      return null;
    }

    return uploadId;
  }

  /**
   * Get the multipart open key based on the bucket layout.
   * @throws IOException
   */
  public static String getMultipartOpenKey(String volumeName,
       String bucketName, String keyName, String multipartUploadId,
       OMMetadataManager omMetadataManager, BucketLayout bucketLayout)
      throws IOException {
    if (bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      return omMetadataManager.getMultipartKeyFSO(volumeName, bucketName, keyName, multipartUploadId);
    } else {
      return omMetadataManager.getMultipartKey(volumeName, bucketName, keyName, multipartUploadId);
    }
  }

  /**
   * Check whether key's isMultipartKey flag is set.
   * @param openKeyInfo open key
   * @return true if flag is set, false otherwise.
   */
  public static boolean isMultipartKeySet(OmKeyInfo openKeyInfo) {
    return openKeyInfo.getLatestVersionLocations() != null
        && openKeyInfo.getLatestVersionLocations().isMultipartKey();
  }
}
