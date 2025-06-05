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

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * Common class to do listing of resources after merging
 * rocksDB table cache and actual table.
 */
public class ListIterator {

  /**
   * Interface for iteration of Heap Entries.
   */
  public interface ClosableIterator extends Iterator<HeapEntry>, Closeable {

  }

  /**
   * Entry to be added to the heap.
   */
  public static class HeapEntry implements Comparable<HeapEntry> {
    private final int entryIteratorId;
    private final String tableName;
    private final String key;
    private final Object value;

    HeapEntry(int entryIteratorId, String tableName, String key,
              Object value) {
      this.entryIteratorId = entryIteratorId;
      this.tableName = tableName;
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return this.key;
    }

    private int getEntryIteratorId() {
      return this.entryIteratorId;
    }

    public String getTableName() {
      return tableName;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public int compareTo(HeapEntry other) {
      return Comparator.comparing(HeapEntry::getKey)
          .thenComparing(HeapEntry::getEntryIteratorId).compare(this, other);
    }

    @Override
    public boolean equals(Object other) {

      if (!(other instanceof HeapEntry)) {
        return false;
      }


      HeapEntry that = (HeapEntry) other;
      return this.compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }
  }

  /**
   * Iterator for DB entries from a given rocksDB table.
   */
  public static class DbTableIter<Value> implements
      ClosableIterator {
    private final int entryIteratorId;
    private final TableIterator<String,
        ? extends Table.KeyValue<String, Value>> tableIterator;

    private final Table<String, Value> table;
    private HeapEntry currentEntry;
    private Predicate<String> doesKeyExistInCache;

    DbTableIter(int entryIteratorId, Table<String, Value> table,
                String prefixKey, String startKey,
                Predicate<String> doesKeyExistInCache) throws IOException {
      this.entryIteratorId = entryIteratorId;
      this.table = table;
      this.tableIterator = table.iterator(prefixKey);
      this.currentEntry = null;
      this.doesKeyExistInCache = doesKeyExistInCache;

      // only seek for the start key if the start key is lexicographically
      // after the prefix key. For example
      // Prefix key = 1024/c, Start key = 1024/a
      // then do not seek for the start key
      //
      // on the other hand,
      // Prefix key = 1024/a, Start key = 1024/c
      // then seek for the start key
      if (!StringUtils.isBlank(startKey) &&
          startKey.compareTo(prefixKey) > 0) {
        tableIterator.seek(startKey);
      }
    }

    private void getNextKey() throws IOException {
      while (tableIterator.hasNext() && currentEntry == null) {
        Table.KeyValue<String, Value> entry = tableIterator.next();
        String entryKey = entry.getKey();
        if (!doesKeyExistInCache.test(entryKey)) {
          currentEntry = new HeapEntry(entryIteratorId,
              table.getName(), entryKey, entry.getValue());
        }
      }
    }

    @Override
    public boolean hasNext() {
      try {
        getNextKey();
      } catch (IOException t) {
        throw new UncheckedIOException(t);
      }
      return currentEntry != null;
    }

    @Override
    public HeapEntry next() {
      if (hasNext()) {
        HeapEntry ret = currentEntry;
        currentEntry = null;
        return ret;
      }
      throw new NoSuchElementException();
    }

    @Override
    public void close() throws IOException {
      tableIterator.close();
    }
  }

  /**
   * Iterator for Cache entries in a Dir and File Table.
   */
  public static class CacheIter<Value>
      implements ClosableIterator {
    private final Map<String, Value> cacheKeyMap;

    private final Iterator<Map.Entry<String, Value>>
        cacheCreatedKeyIter;
    private final String prefixKey;
    private final String startKey;
    private final String tableName;
    private final int entryIteratorId;

    CacheIter(int entryIteratorId, String tableName,
              Iterator<Map.Entry<CacheKey<String>,
                  CacheValue<Value>>> cacheIter, String startKey,
              String prefixKey) {
      this.cacheKeyMap = new TreeMap<>();
      this.startKey = startKey;
      this.prefixKey = prefixKey;
      this.tableName = tableName;
      this.entryIteratorId = entryIteratorId;

      populateCacheMap(cacheIter);

      cacheCreatedKeyIter = cacheKeyMap.entrySet().stream().filter(e -> e.getValue() != null).iterator();
    }

    private void populateCacheMap(Iterator<Map.Entry<CacheKey<String>,
        CacheValue<Value>>> cacheIter) {
      while (cacheIter.hasNext()) {
        Map.Entry<CacheKey<String>, CacheValue<Value>> entry =
            cacheIter.next();
        String cacheKey = entry.getKey().getCacheKey();
        Value cacheOmInfo = entry.getValue().getCacheValue();

        // Copy cache value to local copy and work on it
        if (cacheOmInfo instanceof CopyObject) {
          cacheOmInfo = ((CopyObject<Value>) cacheOmInfo).copyObject();
        }
        if (StringUtils.isBlank(startKey)) {
          // startKey is null or empty, then the seekKeyInDB="1024/"
          if (cacheKey.startsWith(prefixKey)) {
            cacheKeyMap.put(cacheKey, cacheOmInfo);
          }
        } else {
          // startKey not empty, then the seekKeyInDB="1024/b" and
          // seekKeyInDBWithOnlyParentID = "1024/". This is to avoid case of
          // parentID with "102444" cache entries.
          // Here, it has to list all the keys after "1024/b" and requires >=0
          // string comparison.
          if (cacheKey.startsWith(prefixKey) &&
              cacheKey.compareTo(startKey) >= 0) {
            cacheKeyMap.put(cacheKey, cacheOmInfo);
          }
        }
      }
    }

    public boolean doesKeyExistInCache(String key) {
      return cacheKeyMap.containsKey(key);
    }

    @Override
    public boolean hasNext() {
      return cacheCreatedKeyIter.hasNext();
    }

    @Override
    public HeapEntry next() {
      Map.Entry<String, Value> entry = cacheCreatedKeyIter.next();
      return new HeapEntry(this.entryIteratorId, this.tableName,
          entry.getKey(), entry.getValue());
    }

    @Override
    public void close() {
      // Nothing to close here
    }
  }

  /**
   * Implement lexicographical sorting of the file status by sorting file status
   * across multiple lists. Each of these lists are sorted internally.
   *
   * This class implements sorted output by implementing a min heap based
   * iterator where the initial element from each of sorted list is inserted.
   *
   * The least entry is removed and the next entry from the same list from
   * which the entry is removed is added into the list.
   *
   * For example
   * RawDir   - a1, a3, a5, a7
   * RawFile  - a2, a4, a6, a8
   *
   * Min Heap is initially composed of {(a1, RawDir), (a2, RawFile)}
   * The least element is removed i.e a1 and then next entry from RawDir
   * is inserted into minheap resulting in {(a2, RawFile), (a3, RawDir)}
   *
   * This process is repeated till both the lists are exhausted.
   */
  public static class MinHeapIterator implements ClosableIterator {
    private final PriorityQueue<HeapEntry> minHeap = new PriorityQueue<>();
    private final ArrayList<ClosableIterator> iterators = new ArrayList<>();

    MinHeapIterator(OMMetadataManager omMetadataManager, String prefixKey,
                    BucketLayout bucketLayout, String startKey,
                    String volumeName, String bucketName) throws IOException {

      this(omMetadataManager, prefixKey, startKey, volumeName,
          bucketName, omMetadataManager.getDirectoryTable(),
          omMetadataManager.getKeyTable(bucketLayout));
    }

    MinHeapIterator(OMMetadataManager omMetadataManager, String prefixKey,
                    String startKey, String volumeName, String bucketName,
                    Table... tables) throws IOException {
      omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
          bucketName);
      try {
        int iteratorId = 0;
        for (Table table : tables) {
          CacheIter cacheIter = new CacheIter<>(iteratorId, table.getName(),
              table.cacheIterator(), startKey, prefixKey);
          Predicate<String> doesKeyExistInCache = cacheIter::doesKeyExistInCache;
          iterators.add(cacheIter);
          iteratorId++;
          iterators.add(new DbTableIter<>(iteratorId, table, prefixKey,
              startKey, doesKeyExistInCache));
          iteratorId++;
        }
      } finally {
        omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
            bucketName);
      }

      // Insert the element from each of the iterator
      for (Iterator<HeapEntry> iter : iterators) {
        try {
          if (iter.hasNext()) {
            minHeap.add(iter.next());
          }
        } catch (UncheckedIOException e) {
          throw e.getCause();
        }
      }

    }

    @Override
    public boolean hasNext() {
      return !minHeap.isEmpty();
    }

    @Override
    public HeapEntry next() {
      HeapEntry heapEntry = minHeap.remove();
      // remove the least element and
      // reinsert the next element from the same iterator
      Iterator<HeapEntry> iter = iterators.get(heapEntry.getEntryIteratorId());
      if (iter.hasNext()) {
        minHeap.add(iter.next());
      }

      return heapEntry;
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeQuietly(iterators);
    }
  }
}
