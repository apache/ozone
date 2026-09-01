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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

/**
 * Searches the noncurrent versions of a single key in the versionedKeyTable.
 *
 * <p>A point lookup by versionId is a plain {@link Table#get}, which consults
 * the cache; the searches here are for versions addressed by an attribute
 * rather than by id, which no key of the table encodes.
 */
public final class NoncurrentVersions {

  private NoncurrentVersions() {
  }

  /**
   * Whether the key still has any noncurrent version.
   *
   * <p>Not a plain prefix iteration: {@link Table#iterator} reads RocksDB
   * alone, so a version demoted by a transaction the double buffer has not
   * flushed yet is invisible to it. Answering "none left" then removes the
   * key's delete marker and promotes that version back to current,
   * resurrecting an object the user deleted.
   */
  public static boolean any(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName)
      throws IOException {
    return newestMatching(omMetadataManager, volumeName, bucketName, keyName,
        keyInfo -> true) != null;
  }

  /**
   * Returns the key's null version as a (dbKey, keyInfo) pair, or null if the
   * key has no noncurrent null version. A key has at most one.
   */
  public static Pair<String, OmKeyInfo> nullVersion(
      OMMetadataManager omMetadataManager, String volumeName,
      String bucketName, String keyName) throws IOException {
    return newestMatching(omMetadataManager, volumeName, bucketName, keyName,
        OmKeyInfo::isNullVersionRecord);
  }

  /**
   * Returns the newest noncurrent version of the key that satisfies
   * {@code filter}, or null if there is none. Versions of a key are adjacent
   * and ordered newest first, so the smallest matching dbKey wins.
   *
   * <p>Both the table cache and the DB are searched, and neither can stand in
   * for the other:
   *
   * <ul>
   *   <li>{@link Table#iterator} reads RocksDB directly and does not consult
   *       the cache, so a version written by a transaction that the double
   *       buffer has not flushed yet is invisible to it, and a version removed
   *       by such a transaction still appears in it;</li>
   *   <li>the cache only holds the current flush window, so every older
   *       version of the key exists in the DB only.</li>
   * </ul>
   *
   * <p>The search does not stop at the first cache match either. Versions are
   * demoted into the versionedKeyTable in increasing versionId order, so in
   * practice a cached version is newer than every flushed one, but that is a
   * property of the write paths rather than something enforced here, and
   * relying on it would silently promote the wrong version if a later write
   * path broke it. The DB search costs one seek, since it stops at the first
   * match.
   */
  public static Pair<String, OmKeyInfo> newestMatching(
      OMMetadataManager omMetadataManager, String volumeName,
      String bucketName, String keyName, Predicate<OmKeyInfo> filter)
      throws IOException {
    final String prefix = omMetadataManager.getVersionedOzoneKeyPrefix(
        volumeName, bucketName, keyName);
    final Table<String, OmKeyInfo> table =
        omMetadataManager.getVersionedKeyTable();

    String bestKey = null;
    OmKeyInfo bestValue = null;

    // Entries of transactions that are not flushed yet. The cache is not
    // sorted, so it has to be scanned; it only holds this table's writes from
    // the current flush window.
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> cacheIterator =
        table.cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry = cacheIterator.next();
      String dbKey = entry.getKey().getCacheKey();
      OmKeyInfo value = entry.getValue().getCacheValue();
      // a null value is a tombstone: the version is deleted but not flushed
      if (value == null || !dbKey.startsWith(prefix) || !filter.test(value)) {
        continue;
      }
      if (bestKey == null || dbKey.compareTo(bestKey) < 0) {
        bestKey = dbKey;
        bestValue = value;
      }
    }

    try (Table.KeyValueIterator<String, OmKeyInfo> versions = table.iterator(prefix)) {
      while (versions.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = versions.next();
        String dbKey = entry.getKey();
        CacheValue<OmKeyInfo> cached = table.getCacheValue(new CacheKey<>(dbKey));
        if (cached != null && cached.getCacheValue() == null) {
          continue;
        }
        OmKeyInfo value = cached != null ? cached.getCacheValue() : entry.getValue();
        if (!filter.test(value)) {
          continue;
        }
        if (bestKey == null || dbKey.compareTo(bestKey) < 0) {
          bestKey = dbKey;
          bestValue = value;
        }
        // DB entries ascend, so the first match is the smallest one in the DB
        break;
      }
    }
    return bestKey == null ? null : Pair.of(bestKey, bestValue);
  }
}
