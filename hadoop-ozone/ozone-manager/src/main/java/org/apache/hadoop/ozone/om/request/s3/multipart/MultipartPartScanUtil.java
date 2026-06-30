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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;

/**
 * Cache-aware scanner for multipart parts table rows.
 */
public final class MultipartPartScanUtil {

  private MultipartPartScanUtil() {
  }

  public static SortedMap<Integer, OmMultipartPartInfo> scanParts(
      OMMetadataManager omMetadataManager, String uploadId) throws IOException {
    SortedMap<Integer, OmMultipartPartInfo> parts = new TreeMap<>();
    OmMultipartPartKey prefix = OmMultipartPartKey.prefix(uploadId);

    try (TableIterator<OmMultipartPartKey,
        ? extends Table.KeyValue<OmMultipartPartKey, OmMultipartPartInfo>>
        iterator = omMetadataManager.getMultipartPartsTable().iterator(prefix)) {
      while (iterator.hasNext()) {
        Table.KeyValue<OmMultipartPartKey, OmMultipartPartInfo> kv =
            iterator.next();
        if (kv == null) {
          continue;
        }
        OmMultipartPartKey key = kv.getKey();
        if (!uploadId.equals(key.getUploadId())) {
          break;
        }
        if (key.hasPartNumber()) {
          parts.put(key.getPartNumber(), kv.getValue());
        }
      }
    }

    Iterator<Map.Entry<CacheKey<OmMultipartPartKey>,
        CacheValue<OmMultipartPartInfo>>> cacheIterator =
        omMetadataManager.getMultipartPartsTable().cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<OmMultipartPartKey>, CacheValue<OmMultipartPartInfo>>
          cacheEntry = cacheIterator.next();
      OmMultipartPartKey key = cacheEntry.getKey().getCacheKey();
      if (!uploadId.equals(key.getUploadId()) || !key.hasPartNumber()) {
        continue;
      }
      OmMultipartPartInfo value = cacheEntry.getValue().getCacheValue();
      if (value == null) {
        parts.remove(key.getPartNumber());
      } else {
        parts.put(key.getPartNumber(), value);
      }
    }

    return parts;
  }

  public static List<OmMultipartPartKey> getPartKeys(String uploadId,
      SortedMap<Integer, OmMultipartPartInfo> parts) {
    List<OmMultipartPartKey> partKeys = new ArrayList<>(parts.size());
    for (Integer partNumber : parts.keySet()) {
      partKeys.add(OmMultipartPartKey.of(uploadId, partNumber));
    }
    return partKeys;
  }

  public static void addPartCleanupCacheEntries(
      OMMetadataManager omMetadataManager,
      List<OmMultipartPartKey> partKeys, long transactionLogIndex) {
    for (OmMultipartPartKey partKey : partKeys) {
      omMetadataManager.getMultipartPartsTable().addCacheEntry(
          new CacheKey<>(partKey), CacheValue.get(transactionLogIndex));
    }
  }

  public static long getReplicatedSize(
      SortedMap<Integer, OmMultipartPartInfo> parts,
      ReplicationConfig replicationConfig) {
    long replicatedSize = 0;
    for (OmMultipartPartInfo part : parts.values()) {
      replicatedSize += QuotaUtil.getReplicatedSize(
          part.getDataSize(), replicationConfig);
    }
    return replicatedSize;
  }

  public static List<OmKeyInfo> toOmKeyInfoList(
      SortedMap<Integer, OmMultipartPartInfo> parts, String volumeName,
      String bucketName, String keyName, ReplicationConfig replicationConfig) {
    List<OmKeyInfo> keyInfos = new ArrayList<>(parts.size());
    for (OmMultipartPartInfo part : parts.values()) {
      keyInfos.add(part.toOmKeyInfo(volumeName, bucketName, keyName,
          replicationConfig));
    }
    return keyInfos;
  }
}
