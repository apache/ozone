/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.NoSuchElementException;
import java.util.Collection;
import java.util.Collections;


import static org.apache.hadoop.ozone.om.lock.
    OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Helper class for fetching List Status for a path.
 */
public class OzoneListStatusHelper {
  /**
   * Interface to get the File Status for a path.
   */
  @FunctionalInterface
  public interface GetFileStatusHelper {
    OzoneFileStatus apply(OmKeyArgs args, String clientAddress,
                          boolean skipFileNotFoundError) throws IOException;
  }

  /**
   * Interface for iteration of Heap Entries.
   */
  public interface ClosableIterator extends Iterator<HeapEntry>, Closeable {

  }

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneListStatusHelper.class);

  private final OMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final GetFileStatusHelper getStatusHelper;

  OzoneListStatusHelper(OMMetadataManager metadataManager, long scmBlockSize,
                        GetFileStatusHelper func) {
    this.metadataManager = metadataManager;
    this.scmBlockSize = scmBlockSize;
    this.getStatusHelper = func;
  }

  public Collection<OzoneFileStatus> listStatusFSO(OmKeyArgs args,
      String startKey, long numEntries, String clientAddress)
      throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");

    boolean listKeysMode = false;
    final String volumeName = args.getVolumeName();
    final String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    String prefixKey = keyName;

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        metadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Volume:{} Bucket:{} does not exist",
            volumeName, bucketName);
      }
      return new ArrayList<>();
    }

    // Determine if the prefixKey is determined from the startKey
    // if the keyName is null
    if (StringUtils.isNotBlank(startKey)) {
      if (StringUtils.isNotBlank(keyName)) {
        if (!listKeysMode &&
            !OzoneFSUtils.isImmediateChild(keyName, startKey)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("StartKey {} is not an immediate child of keyName {}. " +
                "Returns empty list", startKey, keyName);
          }
          return new ArrayList<>();
        }
      } else {
        keyName = OzoneFSUtils.getParentDir(startKey);
        prefixKey = keyName;
        args = args.toBuilder()
            .setKeyName(keyName)
            .setSortDatanodesInPipeline(false)
            .build();
      }
    }

    OzoneFileStatus fileStatus =
        getStatusHelper.apply(args, clientAddress, listKeysMode);

    String dbPrefixKey;
    if (fileStatus == null) {
      // if the file status is null, prefix is a not a valid filesystem path
      // this should only work in list keys mode.
      // fetch the db key based on the prefix path.
      dbPrefixKey = getDbKey(keyName, args, omBucketInfo);
      prefixKey = OzoneFSUtils.getParentDir(keyName);
    } else {
      // If the keyname is a file just return one entry
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus);
      }

      // fetch the db key based on parent prefix id.
      long id = getId(fileStatus, omBucketInfo);
      dbPrefixKey = metadataManager.getOzonePathKey(id, "");
    }

    // Determine startKeyPrefix for DB iteration
    String startKeyPrefix =
        Strings.isNullOrEmpty(startKey) ? "" :
            getDbKey(startKey, args, omBucketInfo);

    TreeMap<String, OzoneFileStatus> map = new TreeMap<>();

    BucketLayout bucketLayout = omBucketInfo.getBucketLayout();

    // fetch the sorted output using a min heap iterator where
    // every remove from the heap will give the smallest entry.
    try (MinHeapIterator heapIterator = new MinHeapIterator(metadataManager,
        dbPrefixKey, bucketLayout, startKeyPrefix, volumeName, bucketName)) {

      while (map.size() < numEntries && heapIterator.hasNext()) {
        HeapEntry entry = heapIterator.next();
        OzoneFileStatus status = entry.getStatus(prefixKey,
            scmBlockSize, volumeName, bucketName);
        map.put(entry.key, status);
      }
    }

    return map.values();
  }

  private String getDbKey(String key, OmKeyArgs args,
                          OmBucketInfo omBucketInfo) throws IOException {
    long startKeyParentId;
    String parent = OzoneFSUtils.getParentDir(key);

    // the keyname is not a valid filesystem path.
    // determine the parent prefix by fetching the
    OmKeyArgs startKeyArgs = args.toBuilder()
        .setKeyName(parent)
        .setSortDatanodesInPipeline(false)
        .build();
    OzoneFileStatus fileStatusInfo = getStatusHelper.apply(startKeyArgs,
        null, true);
    Preconditions.checkNotNull(fileStatusInfo);
    startKeyParentId = getId(fileStatusInfo, omBucketInfo);
    return metadataManager.
        getOzonePathKey(startKeyParentId, OzoneFSUtils.getFileName(key));
  }

  private long getId(OzoneFileStatus fileStatus, OmBucketInfo omBucketInfo) {
    if (fileStatus.getKeyInfo() != null) {
      return fileStatus.getKeyInfo().getObjectID();
    } else {
      // list root directory.
      return omBucketInfo.getObjectID();
    }
  }

  /**
   * Enum of types of entries in the heap.
   */
  public enum EntryType {
    DIR_CACHE,
    FILE_CACHE,
    RAW_DIR_DB,
    RAW_FILE_DB;

    public boolean isDir() {
      switch (this) {
      case DIR_CACHE:
      case RAW_DIR_DB:
        return true;
      case FILE_CACHE:
      case RAW_FILE_DB:
        return false;
      default:
        throw new IllegalArgumentException();
      }
    }
  }

  /**
   * Entry to be added to the heap.
   */
  private static class HeapEntry implements Comparable<HeapEntry> {
    private final EntryType entryType;
    private final String key;
    private final Object value;

    HeapEntry(EntryType entryType, String key, Object value) {
      Preconditions.checkArgument(
          value instanceof OmDirectoryInfo ||
              value instanceof OmKeyInfo);
      this.entryType = entryType;
      this.key = key;
      this.value = value;
    }

    public int compareTo(HeapEntry other) {
      return this.key.compareTo(other.key);
    }

    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }

      if (!(other instanceof HeapEntry)) {
        return false;
      }


      HeapEntry that = (HeapEntry) other;
      return this.key.equals(that.key);
    }

    public int hashCode() {
      return key.hashCode();
    }

    public OzoneFileStatus getStatus(String prefixPath, long scmBlockSize,
                                     String volumeName, String bucketName) {
      OmKeyInfo keyInfo;
      if (entryType.isDir()) {
        Preconditions.checkArgument(value instanceof OmDirectoryInfo);
        OmDirectoryInfo dirInfo = (OmDirectoryInfo) value;
        String dirName = OMFileRequest.getAbsolutePath(prefixPath,
            dirInfo.getName());
        keyInfo = OMFileRequest.getOmKeyInfo(volumeName,
            bucketName, dirInfo, dirName);
      } else {
        Preconditions.checkArgument(value instanceof OmKeyInfo);
        keyInfo = (OmKeyInfo) value;
        keyInfo.setFileName(keyInfo.getKeyName());
        String fullKeyPath = OMFileRequest.getAbsolutePath(prefixPath,
            keyInfo.getKeyName());
        keyInfo.setKeyName(fullKeyPath);
      }
      return new OzoneFileStatus(keyInfo, scmBlockSize, entryType.isDir());
    }
  }

  /**
   * Iterator for DB entries in a Dir and File Table.
   */
  private static class RawIter<Value> implements ClosableIterator {
    private final EntryType iterType;
    private final String prefixKey;
    private final TableIterator<String,
        ? extends Table.KeyValue<String, Value>> tableIterator;

    private final Table<String, Value> table;
    private HeapEntry currentKey;

    RawIter(EntryType iterType, Table<String, Value> table,
            String prefixKey, String startKey) throws IOException {
      this.iterType = iterType;
      this.table = table;
      this.tableIterator = table.iterator();
      this.prefixKey = prefixKey;
      this.currentKey = null;

      if (!StringUtils.isBlank(prefixKey)) {
        tableIterator.seek(prefixKey);
      }

      if (!StringUtils.isBlank(startKey)) {
        tableIterator.seek(startKey);
      }

      getNextKey();
    }

    private void getNextKey() throws IOException {
      while (tableIterator.hasNext() && currentKey == null) {
        Table.KeyValue<String, Value> entry = tableIterator.next();
        String entryKey = entry.getKey();
        if (entryKey.startsWith(prefixKey)) {
          if (!KeyManagerImpl.isKeyDeleted(entryKey, table)) {
            currentKey = new HeapEntry(iterType, entryKey, entry.getValue());
          }
        } else {
          // if the prefix key does not match, then break
          // as the iterator is beyond the prefix.
          break;
        }
      }
    }

    public boolean hasNext() {
      try {
        getNextKey();
      } catch (Throwable t) {
        throw new NoSuchElementException();
      }
      return currentKey != null;
    }

    public HeapEntry next() {
      try {
        getNextKey();
      } catch (Throwable t) {
        throw new NoSuchElementException();
      }
      HeapEntry ret = currentKey;
      currentKey = null;

      return ret;
    }

    public void close() throws IOException {
      tableIterator.close();
    }
  }

  /**
   * Iterator for Cache entries in a Dir and File Table.
   */
  private static class CacheIter<Value extends WithParentObjectId>
      implements ClosableIterator {
    private final Map<String, Value> cacheKeyMap;
    private final Iterator<Map.Entry<String, Value>>
        cacheCreatedKeyIter;
    private final Iterator<Map.Entry<CacheKey<String>, CacheValue<Value>>>
        cacheIter;
    private final String prefixKey;
    private final String startKey;
    private final EntryType entryType;

    CacheIter(EntryType entryType, Iterator<Map.Entry<CacheKey<String>,
        CacheValue<Value>>> cacheIter, String startKey, String prefixKey) {
      this.cacheKeyMap = new TreeMap<>();

      this.cacheIter = cacheIter;
      this.startKey = startKey;
      this.prefixKey = prefixKey;
      this.entryType = entryType;

      getCacheValues();

      cacheCreatedKeyIter = cacheKeyMap.entrySet().iterator();
    }

    private void getCacheValues() {
      while (cacheIter.hasNext()) {
        Map.Entry<CacheKey<String>, CacheValue<Value>> entry =
            cacheIter.next();
        String cacheKey = entry.getKey().getCacheKey();
        Value cacheOmInfo = entry.getValue().getCacheValue();
        // cacheOmKeyInfo is null if an entry is deleted in cache
        if (cacheOmInfo == null) {
          continue;
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

    public boolean hasNext() {
      return cacheCreatedKeyIter.hasNext();
    }

    public HeapEntry next() {
      Map.Entry<String, Value> entry = cacheCreatedKeyIter.next();
      return new HeapEntry(entryType, entry.getKey(), entry.getValue());
    }

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
   * THe least element is removed i.e a1 and then next entry from RawDir
   * is inserted into minheap resulting in {(a2, RawFile), (a3, RawDir)}
   *
   * This process is repeated till both the lists are exhausted.
   */
  private static class MinHeapIterator implements ClosableIterator {
    private final PriorityQueue<HeapEntry> minHeap = new PriorityQueue<>();
    private final ArrayList<ClosableIterator> iterators = new ArrayList<>();

    MinHeapIterator(OMMetadataManager omMetadataManager, String prefixKey,
                    BucketLayout bucketLayout, String startKey,
                    String volumeName, String bucketName) throws IOException {

      omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
          bucketName);

      // Initialize all the iterators
      iterators.add(EntryType.DIR_CACHE.ordinal(),
          new CacheIter<>(EntryType.DIR_CACHE,
              omMetadataManager.getDirectoryTable().cacheIterator(),
              startKey, prefixKey));

      iterators.add(EntryType.FILE_CACHE.ordinal(),
          new CacheIter<>(EntryType.FILE_CACHE,
              omMetadataManager.getKeyTable(bucketLayout).cacheIterator(),
              startKey, prefixKey));

      iterators.add(EntryType.RAW_DIR_DB.ordinal(),
          new RawIter<>(EntryType.RAW_DIR_DB,
              omMetadataManager.getDirectoryTable(),
              prefixKey, startKey));

      iterators.add(EntryType.RAW_FILE_DB.ordinal(),
          new RawIter<>(EntryType.RAW_FILE_DB,
              omMetadataManager.getKeyTable(bucketLayout),
              prefixKey, startKey));

      omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);

      // Insert the element from each of the iterator
      for (Iterator<HeapEntry> iter : iterators) {
        if (iter.hasNext()) {
          minHeap.add(iter.next());
        }
      }
    }


    public boolean hasNext() {
      return !minHeap.isEmpty();
    }

    public HeapEntry next() {
      HeapEntry heapEntry = minHeap.remove();
      // remove the least element and
      // reinsert the next element from the same iterator
      Iterator<HeapEntry> iter = iterators.get(heapEntry.entryType.ordinal());
      if (iter.hasNext()) {
        minHeap.add(iter.next());
      }

      return heapEntry;
    }

    public void close() throws IOException {
      for (ClosableIterator iterator : iterators) {
        iterator.close();
      }
    }
  }
}
