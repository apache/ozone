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
import java.nio.file.Paths;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.NoSuchElementException;


import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Helper class for fetching List Status for a path.
 */
public class OzoneListStatusHelper {

  /**
   * Interface to get the File Status.
   */
  @FunctionalInterface
  public interface GetFileStatusHelper {
    OzoneFileStatus apply(OmKeyArgs args, String clientAddress,
                          boolean skipFileNotFoundError) throws IOException;
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

  public List<OzoneFileStatus> listStatusFSO(OmKeyArgs args,
      boolean recursive, String startKey, long numEntries,
      String clientAddress)
      throws IOException {
    Preconditions.checkArgument(!recursive);
    Preconditions.checkNotNull(args, "Key args can not be null");

    if (numEntries <= 0) {
      return new ArrayList<>();
    }

    final String volumeName = args.getVolumeName();
    final String bucketName = args.getBucketName();
    final String keyName = args.getKeyName();
    String prefixKey = keyName;

    /**
     * a) If the keyname is a file just return one entry
     * b) if the keyname is root, then return the value of the bucket
     * c) if the keyname is a different bucket than root,
     * fetch the direcoty parent id
     *
     * if the startkey exists
     * a) check the start key is a child ot key, else return emptry list
     * b) chekc if the start key is a child chil of keynae,
     * then reset the key to parent of start key
     * c) if start key is non existent then seek to the neatest key
     * d) if the keyname is not a dir or a file, it can either be
     * invalid name or a prefix path
     *     in case, this is called as part of listStatus fail as the
     *     real dir/file should exist
     *     else, try to find the parent of keyname and use that as the prefix,
     *     use the rest of the path to construct prefix path
     */


    if (StringUtils.isNotBlank(keyName) && StringUtils.isNotBlank(startKey) &&
        !OzoneFSUtils.isImmediateChild(keyName, startKey)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("StartKey {} is not an immediate child of keyName {}. " +
            "Returns empty list", startKey, keyName);
      }
      return Collections.emptyList();
    }

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        metadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("StartKey {} is not an immediate child of keyName {}. " +
            "Returns empty list", startKey, keyName);
      }
      return Collections.emptyList();
    }

    BucketLayout bucketLayout = omBucketInfo.getBucketLayout();

    OzoneFileStatus fileStatus =
        getStatusHelper.apply(args, clientAddress, true);

    String dbPrefixKey;
    if (fileStatus == null) {
      dbPrefixKey = getDbKey(keyName, args, omBucketInfo);
      prefixKey = OzoneFSUtils.getParentDir(keyName);
    } else {
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus);
      }
      long id = getId(fileStatus, omBucketInfo);
      dbPrefixKey = metadataManager.getOzonePathKey(id, "");
    }

    String startKeyPrefix =
        Strings.isNullOrEmpty(startKey) ? "" :
            getDbKey(startKey, args, omBucketInfo);

    TreeMap<String, OzoneFileStatus> cacheKeyMap = new TreeMap<>();

    try (MinHeapIterator heapIterator =
             new MinHeapIterator(metadataManager, dbPrefixKey, bucketLayout,
                 startKeyPrefix, volumeName, bucketName)) {

      while (cacheKeyMap.size() < numEntries && heapIterator.hasNext()) {
        HeapEntry entry = heapIterator.next();
        OzoneFileStatus status = entry.getStatus(prefixKey,
            scmBlockSize, volumeName, bucketName);
        LOG.info("returning status:{} keyname:{} startkey:{} numEntries:{}",
            status, prefixKey, startKey, numEntries);
        cacheKeyMap.put(entry.getKey(), status);
      }
    }

    return new ArrayList<>(cacheKeyMap.values());
  }


  private String getDbKey(String key, OmKeyArgs args,
                          OmBucketInfo omBucketInfo) throws IOException {
    long startKeyParentId;
    java.nio.file.Path file = Paths.get(key);
    String parent = OzoneFSUtils.getParentDir(key);

    OmKeyArgs startKeyArgs = args.toBuilder()
        .setKeyName(parent)
        .setSortDatanodesInPipeline(false)
        .build();
    OzoneFileStatus fileStatusInfo = getStatusHelper.apply(startKeyArgs,
        null, true);
    Preconditions.checkNotNull(fileStatusInfo);
    startKeyParentId = getId(fileStatusInfo, omBucketInfo);
    return metadataManager.
        getOzonePathKey(startKeyParentId, file.getFileName().toString());
  }

  private long getId(OzoneFileStatus fileStatus, OmBucketInfo omBucketInfo) {
    if (fileStatus.getKeyInfo() != null) {
      return fileStatus.getKeyInfo().getObjectID();
    } else {
      // assert root is null
      // list root directory.
      return omBucketInfo.getObjectID();
    }
  }

  /**
   * Enum of types of entries.
   */
  public enum EntryType {
    DIR_CACHE,
    FILE_CACHE,
    RAW_FILE_DB,
    RAW_DIR_DB;

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
   * Entry which is added to heap.
   * @param <T>
   */
  private static class HeapEntry<T extends WithParentObjectId>
      implements Comparable<HeapEntry> {
    private final EntryType entryType;
    private final String key;
    private final T value;

    HeapEntry(EntryType entryType, String key, T value) {
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

    public String getKey() {
      return key;
    }

    public OzoneFileStatus getStatus(String prefixPath, long scmBlockSize,
                                     String volumeName, String bucketName) {
      OmKeyInfo keyInfo;
      if (entryType.isDir()) {
        OmDirectoryInfo dirInfo = (OmDirectoryInfo) value;
        String dirName = OMFileRequest.getAbsolutePath(prefixPath,
            dirInfo.getName());
        keyInfo = OMFileRequest.getOmKeyInfo(volumeName,
            bucketName, dirInfo, dirName);
      } else {
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
   * Iterator class for Ozone keys
   */
  public interface OzoneKeyIterator extends
      Iterator<HeapEntry<? extends WithParentObjectId>>, Closeable {
  }

  /**
   * Raw iterator over db tables.
   * @param <T>
   */
  private static class RawIter<T extends WithParentObjectId>
      implements OzoneKeyIterator {

    private final EntryType iterType;
    private final String prefixKey;

    private final TableIterator<String,
        ? extends Table.KeyValue<String, T>> tableIterator;
    private final Set<String> cacheDeletedKeySet;
    private HeapEntry currentKey;

    RawIter(EntryType iterType, TableIterator<String,
        ? extends Table.KeyValue<String, T>> tableIterator,
            String prefixKey, String startKey,
            Set<String> cacheDeletedKeySet) throws IOException {
      this.iterType = iterType;
      this.tableIterator = tableIterator;
      this.prefixKey = prefixKey;
      this.cacheDeletedKeySet = cacheDeletedKeySet;
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
        Table.KeyValue<String, T> entry = tableIterator.next();
        String entryKey = entry.getKey();
        if (entryKey.startsWith(prefixKey)) {
          if (!cacheDeletedKeySet.contains(entryKey)) {
            currentKey = new HeapEntry(iterType, entryKey, entry.getValue());
          }
        } else {
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

    public void close() {

    }
  }

  /**
   * Iterator for Cache for the database.
   * @param <T>
   */
  private static class CacheIter< T extends WithParentObjectId>
      implements OzoneKeyIterator {
    private final Set<String> cacheDeletedKeySet;
    private final Map<String, T> cacheKeyMap;

    private final Iterator<Map.Entry<String, T>>
        cacheCreatedKeyIter;

    private final Iterator<Map.Entry<CacheKey<String>, CacheValue<T>>>
        cacheIter;

    private final String prefixKey;
    private final String startKey;


    private final EntryType entryType;

    CacheIter(EntryType entryType, Iterator<Map.Entry<CacheKey<String>,
        CacheValue<T>>> cacheIter, String startKey, String prefixKey) {
      this.cacheDeletedKeySet = new TreeSet<>();
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
        Map.Entry<CacheKey<String>, CacheValue<T>> entry =
            cacheIter.next();
        String cacheKey = entry.getKey().getCacheKey();
        T cacheOmInfo = entry.getValue().getCacheValue();
        // cacheOmKeyInfo is null if an entry is deleted in cache
        if (cacheOmInfo == null) {
          cacheDeletedKeySet.add(cacheKey);
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
      Map.Entry<String, T> entry = cacheCreatedKeyIter.next();
      return new HeapEntry(entryType, entry.getKey(), entry.getValue());
    }

    public void close() {

    }

    public Set<String> getDeletedKeySet() {
      return cacheDeletedKeySet;
    }
  }

  /**
   * Implement a min heap iterator to find the smaller
   * lexicographically sorted string.
   */
  private static class MinHeapIterator implements
      Iterator<HeapEntry<? extends WithParentObjectId>>, Closeable {

    private final PriorityQueue<HeapEntry
        <? extends WithParentObjectId>> minHeap = new PriorityQueue<>();
    private final ArrayList<Iterator<HeapEntry
        <? extends WithParentObjectId>>> iterators = new ArrayList<>();

    private final RawIter<OmDirectoryInfo> rawDirIter;
    private final RawIter<OmKeyInfo> rawFileIter;

    private final CacheIter<OmKeyInfo> cacheFileIter;
    private final CacheIter<OmDirectoryInfo> cacheDirIter;

    MinHeapIterator(OMMetadataManager omMetadataManager, String prefixKey,
                    BucketLayout bucketLayout, String startKey,
                    String volumeName, String bucketName) throws IOException {

      omMetadataManager.getLock().acquireReadLock(BUCKET_LOCK, volumeName,
          bucketName);
      cacheDirIter =
          new CacheIter<>(EntryType.DIR_CACHE,
              omMetadataManager.getDirectoryTable().cacheIterator(),
              startKey, prefixKey);

      cacheFileIter =
          new CacheIter<>(EntryType.FILE_CACHE,
              omMetadataManager.getKeyTable(bucketLayout).cacheIterator(),
              startKey, prefixKey);

      rawDirIter =
          new RawIter<>(EntryType.RAW_DIR_DB,
              omMetadataManager.getDirectoryTable().iterator(),
              prefixKey, startKey, cacheDirIter.getDeletedKeySet());

      rawFileIter =
          new RawIter<>(EntryType.RAW_FILE_DB,
              omMetadataManager.getKeyTable(bucketLayout).iterator(),
              prefixKey, startKey, cacheFileIter.getDeletedKeySet());

      omMetadataManager.getLock().releaseReadLock(BUCKET_LOCK, volumeName,
          bucketName);

      iterators.add(EntryType.DIR_CACHE.ordinal(), cacheDirIter);
      iterators.add(EntryType.FILE_CACHE.ordinal(), cacheFileIter);
      iterators.add(EntryType.RAW_FILE_DB.ordinal(), rawFileIter);
      iterators.add(EntryType.RAW_DIR_DB.ordinal(), rawDirIter);
      insertFirstElement();

    }

    public void insertFirstElement() {
      for (Iterator<HeapEntry<? extends WithParentObjectId>> iter :
          iterators) {
        if (iter.hasNext()) {
          minHeap.add(iter.next());
        }
      }
    }

    public boolean hasNext() {
      return !minHeap.isEmpty();
    }

    public HeapEntry<? extends WithParentObjectId> next() {
      HeapEntry<? extends WithParentObjectId> heapEntry = minHeap.remove();
      Iterator<HeapEntry<? extends WithParentObjectId>> iter =
          iterators.get(heapEntry.entryType.ordinal());
      if (iter.hasNext()) {
        minHeap.add(iter.next());
      }

      return heapEntry;
    }

    public void close() throws IOException {
    }
  }
}
