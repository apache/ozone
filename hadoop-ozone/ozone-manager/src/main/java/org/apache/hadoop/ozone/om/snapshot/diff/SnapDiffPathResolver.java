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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.rocksdb.ColumnFamilyHandle;

/**
 * Resolves bucket-relative paths by walking reverse edge links upward from each
 * target object id, with an LRU memo for shared ancestors.
 */
final class SnapDiffPathResolver {

  private static final int DEFAULT_LRU_CAPACITY = 4096;

  private final ManagedRocksDB db;
  private final ColumnFamilyHandle edgesCf;
  private final long bucketObjectId;
  private final int lruCapacity;
  private final LinkedHashMap<Long, String> pathCache;

  SnapDiffPathResolver(ManagedRocksDB db, ColumnFamilyHandle edgesCf, long bucketObjectId) {
    this(db, edgesCf, bucketObjectId, DEFAULT_LRU_CAPACITY);
  }

  SnapDiffPathResolver(ManagedRocksDB db, ColumnFamilyHandle edgesCf, long bucketObjectId,
      int lruCapacity) {
    this.db = db;
    this.edgesCf = edgesCf;
    this.bucketObjectId = bucketObjectId;
    this.lruCapacity = lruCapacity > 0 ? lruCapacity : DEFAULT_LRU_CAPACITY;
    this.pathCache = new LinkedHashMap<Long, String>(this.lruCapacity, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
        return size() > SnapDiffPathResolver.this.lruCapacity;
      }
    };
  }

  List<String> resolvePaths(List<Long> objectIds)
      throws IOException {
    if (objectIds.isEmpty()) {
      return Collections.emptyList();
    }
    Map<Long, byte[]> prefetchedEdges = prefetchEdgeChains(objectIds);
    Map<Long, String> resolvedPaths = new HashMap<>();
    List<String> paths = new ArrayList<>(objectIds.size());
    for (Long objectId : objectIds) {
      paths.add(resolvePath(objectId, prefetchedEdges, resolvedPaths));
    }
    for (Map.Entry<Long, String> resolved : resolvedPaths.entrySet()) {
      pathCache.put(resolved.getKey(), resolved.getValue());
    }
    return paths;
  }

  private String resolvePath(long objectId, Map<Long, byte[]> prefetchedEdges,
      Map<Long, String> resolvedPaths) {
    if (objectId == bucketObjectId) {
      return "";
    }
    String cached = pathCache.get(objectId);
    if (cached != null) {
      return cached;
    }
    String batchResolved = resolvedPaths.get(objectId);
    if (batchResolved != null) {
      return batchResolved;
    }
    populateResolvedPaths(objectId, prefetchedEdges, resolvedPaths);
    return resolvedPaths.get(objectId);
  }

  /**
   * Walks the single-parent chain from {@code objectId} toward the bucket, then
   * materializes bucket-relative paths root-to-leaf for every uncached ancestor
   * into {@code resolvedPaths} for this batch.
   */
  private void populateResolvedPaths(long objectId, Map<Long, byte[]> prefetchedEdges,
      Map<Long, String> resolvedPaths) {
    List<Long> chain = new ArrayList<>();
    List<String> linkValues = new ArrayList<>();
    long current = objectId;
    while (current != bucketObjectId && !pathCache.containsKey(current)
        && !resolvedPaths.containsKey(current)) {
      byte[] value = prefetchedEdges.get(current);
      if (value == null) {
        return;
      }
      chain.add(current);
      linkValues.add(SnapDiffJobStore.decodeEdgeLinkName(value));
      current = SnapDiffJobStore.decodeEdgeLinkParentId(value);
    }
    String suffix = suffixAt(current, resolvedPaths);
    if (suffix == null) {
      return;
    }
    for (int i = chain.size() - 1; i >= 0; i--) {
      suffix = suffix.isEmpty() ? linkValues.get(i) : suffix + OM_KEY_PREFIX + linkValues.get(i);
      resolvedPaths.put(chain.get(i), suffix);
    }
  }

  private String suffixAt(long objectId, Map<Long, String> resolvedPaths) {
    if (objectId == bucketObjectId) {
      return "";
    }
    String cached = pathCache.get(objectId);
    if (cached != null) {
      return cached;
    }
    return resolvedPaths.get(objectId);
  }

  private Map<Long, byte[]> prefetchEdgeChains(List<Long> objectIds) throws IOException {
    Map<Long, byte[]> prefetchedEdges = new HashMap<>();
    List<Long> lookupIds = new ArrayList<>();
    for (Long objectId : objectIds) {
      if (objectId != bucketObjectId && !pathCache.containsKey(objectId)) {
        lookupIds.add(objectId);
      }
    }
    while (!lookupIds.isEmpty()) {
      List<Long> dbLookupIds = new ArrayList<>();
      for (Long lookupId : lookupIds) {
        if (!prefetchedEdges.containsKey(lookupId)) {
          dbLookupIds.add(lookupId);
        }
      }
      if (!dbLookupIds.isEmpty()) {
        List<byte[]> keys = new ArrayList<>(dbLookupIds.size());
        for (Long lookupId : dbLookupIds) {
          keys.add(SnapDiffJobStore.objectIdKey(lookupId));
        }
        List<byte[]> values = SnapDiffJobStore.multiGet(db, edgesCf, keys);
        for (int i = 0; i < dbLookupIds.size(); i++) {
          prefetchedEdges.put(dbLookupIds.get(i), values.get(i));
        }
      }
      List<Long> nextFetch = new ArrayList<>();
      for (Long lookupId : lookupIds) {
        byte[] value = prefetchedEdges.get(lookupId);
        if (value != null) {
          long parent = SnapDiffJobStore.decodeEdgeLinkParentId(value);
          if (parent != bucketObjectId && !pathCache.containsKey(parent)
              && !prefetchedEdges.containsKey(parent)) {
            nextFetch.add(parent);
          }
        }
      }
      lookupIds = nextFetch;
    }
    return prefetchedEdges;
  }

  void clearPathCache() {
    pathCache.clear();
  }

  boolean isPathCached(long objectId) {
    return pathCache.containsKey(objectId);
  }
}
