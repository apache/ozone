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
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;


import static org.apache.hadoop.ozone.om.exceptions.OMException.
    ResultCodes.FILE_NOT_FOUND;

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

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneListStatusHelper.class);

  private final OMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final GetFileStatusHelper getStatusHelper;
  private final ReplicationConfig omDefaultReplication;

  OzoneListStatusHelper(
      OMMetadataManager metadataManager,
      long scmBlockSize,
      GetFileStatusHelper func,
      ReplicationConfig omDefaultReplication
  ) {
    this.metadataManager = metadataManager;
    this.scmBlockSize = scmBlockSize;
    this.getStatusHelper = func;
    this.omDefaultReplication = omDefaultReplication;
  }

  public Collection<OzoneFileStatus> listStatusFSO(OmKeyArgs args,
      String startKey, long numEntries, String clientAddress,
      boolean allowPartialPrefixes) throws IOException {
    Preconditions.checkNotNull(args, "Key args can not be null");

    final String volumeName = args.getVolumeName();
    final String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    String prefixKey = keyName;

    final String volumeKey = metadataManager.getVolumeKey(volumeName);
    final String bucketKey = metadataManager.getBucketKey(volumeName,
            bucketName);

    final OmVolumeArgs volumeInfo = metadataManager.getVolumeTable()
            .get(volumeKey);
    final OmBucketInfo omBucketInfo = metadataManager.getBucketTable()
            .get(bucketKey);

    if (volumeInfo == null || omBucketInfo == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%s does not exist.", (volumeInfo == null) ?
                "Volume : " + volumeName :
                "Bucket: " + volumeName + "/" + bucketName));
      }
      return new ArrayList<>();
    }

    // Determine if the prefixKey is determined from the startKey
    // if the keyName is null
    if (StringUtils.isNotBlank(startKey)) {
      if (StringUtils.isNotBlank(keyName)) {
        if (!OzoneFSUtils.isSibling(keyName, startKey) &&
            !OzoneFSUtils.isImmediateChild(keyName, startKey)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("StartKey {} is not an immediate child or not a sibling"
                + " of keyName {}. Returns empty list", startKey, keyName);
          }
          return new ArrayList<>();
        }
      } else {
        // if the prefix is blank
        keyName = OzoneFSUtils.getParentDir(startKey);
        prefixKey = keyName;
        args = args.toBuilder()
            .setKeyName(keyName)
            .setSortDatanodesInPipeline(false)
            .build();
      }
    }

    OzoneFileStatus fileStatus =
        getStatusHelper.apply(args, clientAddress, allowPartialPrefixes);

    String dbPrefixKey;
    if (fileStatus == null) {
      // if the file status is null, prefix is a not a valid filesystem path
      // this should only work in list keys mode.
      // fetch the db key based on the prefix path.
      try {
        dbPrefixKey = getDbKey(keyName, args, volumeInfo, omBucketInfo);
        prefixKey = OzoneFSUtils.getParentDir(keyName);
      } catch (OMException ome) {
        if (ome.getResult() == FILE_NOT_FOUND) {
          // the parent dir cannot be found return null list
          if (LOG.isDebugEnabled()) {
            LOG.debug("Parent directory of keyName:{} does not exist." +
                "Returns empty list", keyName);
          }
          return new ArrayList<>();
        }
        throw ome;
      }
    } else {
      // If the keyname is a file just return one entry
      if (fileStatus.isFile()) {
        return Collections.singletonList(fileStatus);
      }

      // fetch the db key based on parent prefix id.
      long id = getId(fileStatus, omBucketInfo);
      final long volumeId = volumeInfo.getObjectID();
      final long bucketId = omBucketInfo.getObjectID();
      dbPrefixKey = metadataManager.getOzonePathKey(volumeId, bucketId,
              id, "");
    }

    // Determine startKeyPrefix for DB iteration
    String startKeyPrefix = "";
    try {
      if (!Strings.isNullOrEmpty(startKey)) {
        startKeyPrefix = getDbKey(startKey, args, volumeInfo, omBucketInfo);
      }
    } catch (OMException ome) {
      if (ome.getResult() != FILE_NOT_FOUND) {
        throw ome;
      }
    }

    TreeMap<String, OzoneFileStatus> map = new TreeMap<>();

    BucketLayout bucketLayout = omBucketInfo.getBucketLayout();
    ReplicationConfig replication =
        Optional.ofNullable(omBucketInfo.getDefaultReplicationConfig())
            .map(DefaultReplicationConfig::getReplicationConfig)
            .orElse(omDefaultReplication);

    // fetch the sorted output using a min heap iterator where
    // every remove from the heap will give the smallest entry.
    try (ListIterator.MinHeapIterator heapIterator =
             new ListIterator.MinHeapIterator(metadataManager, dbPrefixKey,
                 bucketLayout, startKeyPrefix, volumeName, bucketName)) {

      try {
        while (map.size() < numEntries && heapIterator.hasNext()) {
          ListIterator.HeapEntry entry = heapIterator.next();
          OzoneFileStatus status = getStatus(prefixKey,
              scmBlockSize, volumeName, bucketName, replication, entry);
          // Caution: DO NOT use putIfAbsent. putIfAbsent undesirably overwrites
          // the value with `status` when the existing value in the map is null.
          if (!map.containsKey(entry.getKey())) {
            map.put(entry.getKey(), status);
          }
        }
      } catch (NoSuchElementException e) {
        throw new IOException(e);
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }

    return map.values().stream().filter(e -> e != null).collect(
        Collectors.toList());
  }

  private OzoneFileStatus getStatus(String prefixPath, long scmBlockSz,
                                    String volumeName, String bucketName,
                                    ReplicationConfig bucketReplication,
                                    ListIterator.HeapEntry entry) {
    if (entry == null || entry.getValue() == null) {
      return null;
    }
    Object value = entry.getValue();
    boolean isDir =
        OmMetadataManagerImpl.DIRECTORY_TABLE.equals(entry.getTableName());
    OmKeyInfo keyInfo;
    if (isDir) {
      Preconditions.checkArgument(value instanceof OmDirectoryInfo);
      OmDirectoryInfo dirInfo = (OmDirectoryInfo) value;
      String dirName = OMFileRequest.getAbsolutePath(prefixPath,
          dirInfo.getName());
      keyInfo = OMFileRequest.getOmKeyInfo(volumeName,
          bucketName, dirInfo, dirName);
      keyInfo.setReplicationConfig(bucketReplication); // always overwrite
    } else {
      Preconditions.checkArgument(value instanceof OmKeyInfo);
      keyInfo = (OmKeyInfo) value;
      keyInfo.setFileName(keyInfo.getKeyName());
      String fullKeyPath = OMFileRequest.getAbsolutePath(prefixPath,
          keyInfo.getKeyName());
      keyInfo.setKeyName(fullKeyPath);
    }
    return new OzoneFileStatus(keyInfo, scmBlockSz, isDir);
  }

  private String getDbKey(String key, OmKeyArgs args,
                          OmVolumeArgs volumeInfo,
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
        null, false);
    Preconditions.checkNotNull(fileStatusInfo);
    startKeyParentId = getId(fileStatusInfo, omBucketInfo);
    final long volumeId = volumeInfo.getObjectID();
    final long bucketId = omBucketInfo.getObjectID();
    return metadataManager.
        getOzonePathKey(volumeId, bucketId, startKeyParentId,
                OzoneFSUtils.getFileName(key));
  }

  private long getId(OzoneFileStatus fileStatus, OmBucketInfo omBucketInfo) {
    if (fileStatus.getKeyInfo() != null) {
      return fileStatus.getKeyInfo().getObjectID();
    } else {
      // list root directory.
      return omBucketInfo.getObjectID();
    }
  }

}
