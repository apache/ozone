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

package org.apache.hadoop.ozone.om.snapshot.diff;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Resolves bucket-relative paths by walking reverse edge links upward from each
 * target object id, with an LRU memo for shared ancestors.
 */
final class SnapDiffPathResolver {

  private static final int DEFAULT_LRU_CAPACITY = 4096;

  private final ManagedRocksDB db;
  private final ColumnFamilyHandle edgesCf;
  private final long bucketObjectId;
  private final LinkedHashMap<Long, String> pathCache;

  SnapDiffPathResolver(ManagedRocksDB db, ColumnFamilyHandle edgesCf, long bucketObjectId) {
    this.db = db;
    this.edgesCf = edgesCf;
    this.bucketObjectId = bucketObjectId;
    this.pathCache = new LinkedHashMap<Long, String>(DEFAULT_LRU_CAPACITY, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
        return size() > DEFAULT_LRU_CAPACITY;
      }
    };
    pathCache.put(bucketObjectId, "");
  }

  String resolvePath(long objectId) throws IOException {
    return resolvePath(objectId, null);
  }

  List<String> resolvePaths(List<Long> objectIds) throws IOException {
    if (objectIds.isEmpty()) {
      return Collections.emptyList();
    }
    Map<Long, byte[]> prefetchedEdges = prefetchEdgeChains(objectIds);
    List<String> paths = new ArrayList<>(objectIds.size());
    for (Long objectId : objectIds) {
      paths.add(resolvePath(objectId, prefetchedEdges));
    }
    return paths;
  }

  private String resolvePath(long objectId, Map<Long, byte[]> prefetchedEdges) throws IOException {
    if (objectId == bucketObjectId) {
      return "";
    }
    String cached = pathCache.get(objectId);
    if (cached != null) {
      return cached;
    }
    populatePathCache(objectId, prefetchedEdges);
    return pathCache.get(objectId);
  }

  /**
   * Walks the single-parent chain from {@code objectId} toward the bucket, then
   * materializes bucket-relative paths root-to-leaf for every uncached ancestor.
   */
  private void populatePathCache(long objectId, Map<Long, byte[]> prefetchedEdges)
      throws IOException {
    List<Long> chain = new ArrayList<>();
    List<byte[]> linkValues = new ArrayList<>();
    long current = objectId;
    while (current != bucketObjectId && !pathCache.containsKey(current)) {
      byte[] value = getEdgeValue(current, prefetchedEdges);
      if (value == null) {
        return;
      }
      chain.add(current);
      linkValues.add(value);
      current = SnapDiffJobStore.decodeEdgeLinkParentId(value);
    }
    String suffix = pathCache.get(current);
    if (suffix == null) {
      return;
    }
    for (int i = chain.size() - 1; i >= 0; i--) {
      String name = SnapDiffJobStore.decodeEdgeLinkName(linkValues.get(i));
      suffix = suffix.isEmpty() ? name : suffix + OM_KEY_PREFIX + name;
      pathCache.put(chain.get(i), suffix);
    }
  }

  private Map<Long, byte[]> prefetchEdgeChains(List<Long> objectIds) throws IOException {
    Map<Long, byte[]> prefetchedEdges = new HashMap<>();
    Set<Long> toFetch = new HashSet<>();
    for (Long objectId : objectIds) {
      if (objectId != bucketObjectId && !pathCache.containsKey(objectId)) {
        toFetch.add(objectId);
      }
    }
    while (!toFetch.isEmpty()) {
      List<Long> lookupIds = new ArrayList<>(toFetch);
      List<byte[]> values = multiGetEdgeValues(lookupIds);
      Set<Long> nextFetch = new HashSet<>();
      for (int i = 0; i < lookupIds.size(); i++) {
        long lookupId = lookupIds.get(i);
        byte[] value = values.get(i);
        prefetchedEdges.put(lookupId, value);
        if (value != null) {
          long parent = SnapDiffJobStore.decodeEdgeLinkParentId(value);
          if (parent != bucketObjectId && !pathCache.containsKey(parent)
              && !prefetchedEdges.containsKey(parent)) {
            nextFetch.add(parent);
          }
        }
      }
      toFetch = nextFetch;
    }
    return prefetchedEdges;
  }

  private List<byte[]> multiGetEdgeValues(List<Long> objectIds) throws IOException {
    if (objectIds.isEmpty()) {
      return Collections.emptyList();
    }
    List<byte[]> keys = new ArrayList<>(objectIds.size());
    for (Long objectId : objectIds) {
      keys.add(SnapDiffJobStore.objectIdKey(objectId));
    }
    List<ColumnFamilyHandle> cfs = Collections.nCopies(objectIds.size(), edgesCf);
    try {
      return db.get().multiGetAsList(cfs, keys);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  private byte[] getEdgeValue(long objectId, Map<Long, byte[]> prefetchedEdges) throws IOException {
    if (prefetchedEdges != null && prefetchedEdges.containsKey(objectId)) {
      return prefetchedEdges.get(objectId);
    }
    try {
      return db.get().get(edgesCf, SnapDiffJobStore.objectIdKey(objectId));
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }
}
