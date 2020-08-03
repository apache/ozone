/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


/**
 * Base class for file requests.
 */
public final class OMFileRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMFileRequest.class);
  private static final long TRANSACTION_ID_SHIFT = 8;
  private static final long DIRECTORY_ROOT_INDEX = Long.MAX_VALUE;

  private OMFileRequest() {
  }

  /**
   * Verify any file/key exist in the given path in the specified
   * volume/bucket by iterating through directory table and key table.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param keyPath
   * @return OMPathInfo - holds.
   * @throws IOException
   */
  public static OMPathInfo verifyDirectoryKeysInPath(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull String volumeName,
          @Nonnull String bucketName, @Nonnull String keyName,
          @Nonnull Path keyPath) throws IOException {

    String leafNodeName = OzoneFSUtils.getFileName(keyName);
    List<String> missing = new ArrayList<>();
    List<OzoneAcl> inheritAcls = new ArrayList<>();
    OMDirectoryResult result = OMDirectoryResult.NONE;

    Iterator<Path> elements = keyPath.iterator();
    // TODO: volume id and bucket id generation logic.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId =
            omMetadataManager.getBucketTable().get(bucketKey).getObjectID();
    long lastKnownParentId = bucketId;
    OmDirectoryInfo parentDirectoryInfo = null;
    String dbDirName = ""; // TODO for logging
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      if (missing.size() > 0) {
        // Add all the sub-dirs to the missing list except the leaf element.
        // For example, /vol1/buck1/a/b/c/d/e/f/file1.txt.
        // Assume /vol1/buck1/a/b/c exists, then add d, e, f into missing list.
        if(elements.hasNext()){
          // skips leaf node.
          missing.add(fileName);
        }
        continue;
      }

      // For example, /vol1/buck1/a/b/c/d/e/f/file1.txt
      // 1. Do lookup on directoryTable. If not exists goto next step.
      // 2. Do look on keyTable. If not exists goto next step.
      // 3. Add 'sub-dir' to missing parents list
      String dbNodeName = omMetadataManager.getOzonePrefixKey(lastKnownParentId,
              fileName);
      OmDirectoryInfo omDirectoryInfo = omMetadataManager.getDirectoryTable().
              get(dbNodeName);
      if (omDirectoryInfo != null) {
        if (elements.hasNext()) {
          lastKnownParentId = omDirectoryInfo.getObjectID();
          parentDirectoryInfo = omDirectoryInfo;
          continue;
        } else {
          // Checked all the sub-dirs till the leaf node.
          // Found a directory in the given path.
          result = OMDirectoryResult.DIRECTORY_EXISTS;
        }
      } else {
        if (parentDirectoryInfo != null) {
          lastKnownParentId = parentDirectoryInfo.getObjectID();
          dbNodeName = omMetadataManager.getOzoneLeafNodeKey(lastKnownParentId,
                  fileName);
          if (omMetadataManager.getKeyTable().isExist(dbNodeName)) {
            if (elements.hasNext()) {
              // Found a file in the given key name.
              result = OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
            } else {
              // Checked all the sub-dirs till the leaf file.
              // Found a file with the given key name.
              result = OMDirectoryResult.FILE_EXISTS;
            }
            break; // Skip directory traversal as it hits key.
          }

          result = OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
          inheritAcls = parentDirectoryInfo.getAcls();

          // TODO: For this need to construct dbDirName
            /*String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
                    bucketName, keyPath.toString());
            LOG.trace("Acls inherited from parent " + dbDirKeyName + " are : "
                    + inheritAcls);*/
        }
        // add to the missing list since this directory doesn't exist.
        if(elements.hasNext()) {
          // skips leaf node.
          missing.add(fileName);
        }
      }
    }

    if (result != OMDirectoryResult.NONE) {
      LOG.trace("verifyFiles in Path : " + "/" + volumeName
              + "/" + bucketName + "/" + keyName + ":" + result);
      return new OMPathInfo(leafNodeName, lastKnownParentId, missing, result,
              inheritAcls);
    }

    if (inheritAcls.isEmpty()) {
      inheritAcls = omMetadataManager.getBucketTable().get(bucketKey)
              .getAcls();
      LOG.trace("Acls inherited from bucket " + bucketName + " are : "
              + inheritAcls);
    }

    LOG.trace("verifyFiles in Path : " + volumeName + "/" + bucketName + "/"
            + keyName + ":" + result);
    // Found no files/ directories in the given path.
    return new OMPathInfo(leafNodeName, lastKnownParentId, missing,
            OMDirectoryResult.NONE, inheritAcls);
  }

  /**
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param omMetadataManager
   * @return
   * @throws IOException
   */
  public static OmKeyInfo getKeyIfExists(String volumeName, String bucketName,
                                   String keyName,
                                   OMMetadataManager omMetadataManager)
          throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId =
            omMetadataManager.getBucketTable().get(bucketKey).getObjectID();

    Iterator<Path> elements = Paths.get(keyName).iterator();
    long lastKnownParentId = bucketId;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      String dbNodeName = omMetadataManager.getOzonePrefixKey(lastKnownParentId,
              fileName);
      OmDirectoryInfo omDirectoryInfo = omMetadataManager.
              getDirectoryTable().get(dbNodeName);
      if (elements.hasNext()) {
        if (omDirectoryInfo != null) {
          lastKnownParentId = omDirectoryInfo.getObjectID();
        } else {
          return null;
        }
      } else {
        return omMetadataManager.
                getKeyTable().get(dbNodeName);
      }
    }
    return null;
  }

  /**
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param omMetadataManager
   * @return
   * @throws IOException
   */
  public static OmDirectoryInfo getLastKnownPrefixIfExists(String volumeName,
                                                           String bucketName,
                                                           String keyName,
                                                           OMMetadataManager omMetadataManager)
          throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    long bucketId =
            omMetadataManager.getBucketTable().get(bucketKey).getObjectID();

    Iterator<Path> elements = Paths.get(keyName).iterator();
    long lastKnownParentId = bucketId;
    OmDirectoryInfo omDirectoryInfo = OmDirectoryInfo.createDirectoryInfo(bucketId);
    short index = 0;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      String dbNodeName = omMetadataManager.getOzonePrefixKey(lastKnownParentId,
              fileName);
      OmDirectoryInfo tmpDirInfo = omMetadataManager.
              getDirectoryTable().get(dbNodeName);
      if (tmpDirInfo == null) {
        break; // there is nor dir exists and return previous known dir.
      }
      lastKnownParentId = tmpDirInfo.getObjectID();
      omDirectoryInfo = tmpDirInfo;
      omDirectoryInfo.setIndex(index++); // TODO: improve this code.
    }
    return omDirectoryInfo;
  }

  public static class OzKeyInfo {
    boolean isDir = true;
    OmKeyInfo omKeyInfo = null;
    long lastKnownParentId = Long.MIN_VALUE;

    OzKeyInfo(long lastKnownParentId){
      this.lastKnownParentId = lastKnownParentId;
    }

    OzKeyInfo(boolean isDir, OmKeyInfo omKeyInfo){
      this.isDir = isDir;
      this.omKeyInfo = omKeyInfo;
    }

    public boolean isDir() {
      return isDir;
    }

    public OmKeyInfo getOmKeyInfo() {
      return omKeyInfo;
    }

    public long getLastKnownParentId() {
      return lastKnownParentId;
    }
  }

  /**
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param omMetadataManager
   * @return
   * @throws IOException
   */
  @Nonnull
  public static OzKeyInfo getOmKeyInfoFromDB(String volumeName,
                                             String bucketName,
                                             String keyName,
                                             OMMetadataManager omMetadataManager)
          throws IOException {
    int totalDirsCount;
    OmKeyInfo keyValue;
    OmDirectoryInfo keyDirInfo =
            OMFileRequest.getLastKnownPrefixIfExists(volumeName, bucketName,
                    keyName, omMetadataManager);
    totalDirsCount = OzoneFSUtils.getFileCount(keyName);
    // check if the key is a directory
    if (keyDirInfo.getIndex() == totalDirsCount - 1) {
      keyValue = OMFileRequest.getKeyInfo(keyDirInfo, keyName);
      return new OzKeyInfo(true, keyValue);
    // check if the immediate parent exists
    } else if (keyDirInfo.getIndex() == totalDirsCount - 2) {
      // check if the key is a file
      String fileName = OzoneFSUtils.getFileName(keyName);
      String dbNodeName =
              omMetadataManager.getOzonePrefixKey(keyDirInfo.getObjectID(),
                      fileName);
      keyValue = omMetadataManager.getKeyTable().get(dbNodeName);
      if (keyValue != null) {
        return new OzKeyInfo(false, keyValue);
      } else {
        // return immediate parent parent id. This parentId can be used to
        // create the key, if necessary.
        return new OzKeyInfo(keyDirInfo.getObjectID());
      }
    }
    // placeholder to avoid null checks
    return new OzKeyInfo(true, null);
  }

  /**
   * Get OMKeyInfo from the Table DB for the given keyName.
   *
   * @param ozoneManager
   * @param trxnLogIndex
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param omMetaMgr
   * @param omClientRequest
   * @return OmKeyInfo
   * @throws IOException
   */
  @Nullable
  public static OmKeyInfo getOmKeyInfoFromDB(OzoneManager ozoneManager,
                                             long trxnLogIndex,
                                             String volumeName,
                                             String bucketName,
                                             String keyName,
                                             OMMetadataManager omMetaMgr,
                                             OMClientRequest omClientRequest)
          throws IOException {
    // Check if Key already exists in KeyTable and this transaction is a
    // replay.
    OmKeyInfo dbKeyInfo = OMFileRequest.getKeyIfExists(volumeName, bucketName
            , keyName, omMetaMgr);
    if (dbKeyInfo != null) {
      // Check if this transaction is a replay of ratis logs.
      // We check only the KeyTable here and not the OpenKeyTable. In case
      // this transaction is a replay but the transaction was not committed
      // to the KeyTable, then we recreate the key in OpenKey table. This is
      // okay as all the subsequent transactions would also be replayed and
      // the openKey table would eventually reach the same state.
      // The reason we do not check the OpenKey table is to avoid a DB read
      // in regular non-replay scenario.
      if (omClientRequest.isReplay(ozoneManager, dbKeyInfo, trxnLogIndex)) {
        // Replay implies the response has already been returned to
        // the client. So take no further action and return a dummy response.
        throw new OMReplayException();
      }
    }
    return dbKeyInfo;
  }

  /**
   * Prepare OmKeyInfo from OmDirectoryInfo.
   *
   * @param directoryInfo
   * @param keyName
   * @return OmKeyInfo
   */
  @Nullable
  public static OmKeyInfo getKeyInfo(OmDirectoryInfo directoryInfo,
                                     String keyName){
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder();
    builder.setParentObjectID(directoryInfo.getParentObjectID());
    builder.setLeafNodeName(directoryInfo.getName());
    builder.setAcls(directoryInfo.getAcls());
    builder.addAllMetadata(directoryInfo.getMetadata());
    builder.setVolumeName(directoryInfo.getVolumeName());
    builder.setBucketName(directoryInfo.getBucketName());
    builder.setCreationTime(directoryInfo.getCreationTime());
    builder.setModificationTime(directoryInfo.getModificationTime());
    builder.setObjectID(directoryInfo.getObjectID());
    builder.setUpdateID(directoryInfo.getUpdateID());
    builder.setKeyName(keyName);
    // TODO: hardcoded below values to directory. Do we need to persist this?
    builder.setReplicationType(HddsProtos.ReplicationType.RATIS);
    builder.setReplicationFactor(HddsProtos.ReplicationFactor.ONE);
    builder.setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())));
    return builder.build();
  }

  /**
   *
   * @param keyInfo
   * @return
   */
  public static OmDirectoryInfo getDirectoryInfo(OmKeyInfo keyInfo){
    OmDirectoryInfo.Builder builder = new OmDirectoryInfo.Builder();
    builder.setParentObjectID(keyInfo.getParentObjectID());
    builder.setAcls(keyInfo.getAcls());
    builder.addAllMetadata(keyInfo.getMetadata());
    builder.setVolumeName(keyInfo.getVolumeName());
    builder.setBucketName(keyInfo.getBucketName());
    builder.setCreationTime(keyInfo.getCreationTime());
    builder.setModificationTime(keyInfo.getModificationTime());
    builder.setObjectID(keyInfo.getObjectID());
    builder.setUpdateID(keyInfo.getUpdateID());
    builder.setName(OzoneFSUtils.getFileName(keyInfo.getKeyName()));
    return builder.build();
  }

  public static String getAbsoluteDirName(String prefixName, String keyName) {
    if (Strings.isNullOrEmpty(prefixName)) {
      return keyName;
    }
    return prefixName.concat(OzoneConsts.OZONE_URI_DELIMITER).concat(keyName);
  }

  /**
   *
   */
  public static long getDirectoryId(
          OMMetadataManager omMetadataManager,
          long parentId, String dirName)
          throws IOException {

    OmDirectoryInfo dirInfo =
            omMetadataManager.getDirectoryTable().get(dirName);
    return dirInfo.getObjectID();
  }

  /**
   * Get the valid base object id given the transaction id.
   * @param id of the transaction
   * @return base object id allocated against the transaction
   */
  public static long getObjIDFromTxId(long id) {
    return id << TRANSACTION_ID_SHIFT;
  }

  /**
   * Generate the valid object id range for the transaction id.
   * The transaction id is left shifted by 8 bits -
   * creating space to create (2^8 - 1) object ids in every request.
   * maxId (right element of Immutable pair) represents the largest
   * object id allocation possible inside the transaction.
   * @param id
   * @return object id range
   */
  public static ImmutablePair<Long, Long> getObjIdRangeFromTxId(long id) {
    long baseId = getObjIDFromTxId(id);
    // 1 less than the baseId for the next transaction
    long maxAvailableId = getObjIDFromTxId(id+1) - 1;
    Preconditions.checkState(maxAvailableId >= baseId,
        "max available id must be atleast equal to the base id.");
    return new ImmutablePair<>(baseId, maxAvailableId);
  }

  /**
   * Class to return the results from verifyFilesInPath.
   * Includes the list of missing intermediate directories and
   * the directory search result code.
   */
  public static class OMPathInfo {
    private OMDirectoryResult directoryResult;
    private String leafNodeName;
    private long lastKnownParentId;
    private long leafNodeObjectId;
    private List<String> missingParents;
    private List<OzoneAcl> acls;

    public OMPathInfo(String leafNodeName, long lastKnownParentId,
                      List missingParents,
                      OMDirectoryResult result,
                      List<OzoneAcl> aclList) {
      this.leafNodeName = leafNodeName;
      this.lastKnownParentId = lastKnownParentId;
      this.missingParents = missingParents;
      this.directoryResult = result;
      this.acls = aclList;
    }

    public String getLeafNodeName() { return leafNodeName; }

    public long getLeafNodeObjectId() {
      return leafNodeObjectId;
    }

    public void setLeafNodeObjectId(long leafNodeObjectId) {
      this.leafNodeObjectId = leafNodeObjectId;
    }

    public void setLastKnownParentId(long lastKnownParentId) {
      this.lastKnownParentId = lastKnownParentId;
    }

    public long getLastKnownParentId() { return lastKnownParentId; }

    public List getMissingParents() {
      return missingParents;
    }

    public OMDirectoryResult getDirectoryResult() {
      return directoryResult;
    }

    public List<OzoneAcl> getAcls() {
      return acls;
    }

    /**
     * indicates if the immediate parent in the path already exists.
     * @return true indicates the parent exists
     */
    public boolean directParentExists() {
      return missingParents.isEmpty();
    }
  }

  /**
   * Return codes used by verifyFilesInPath method.
   */
  enum OMDirectoryResult {

    // In below examples path is assumed as "a/b/c" in volume volume1 and
    // bucket b1.

    // When a directory exists in given path.
    // If we have a directory with name "a/b" we return this enum value.
    DIRECTORY_EXISTS_IN_GIVENPATH,

    // When a file exists in given path.
    // If we have a file with name "a/b" we return this enum value.
    FILE_EXISTS_IN_GIVENPATH,

    // When file already exists with the given path.
    // If we have a file with name "a/b/c" we return this enum value.
    FILE_EXISTS,

    // When directory exists with the given path.
    // If we have a file with name "a/b/c" we return this enum value.
    DIRECTORY_EXISTS,

    // If no file/directory exists with the given path.
    // If we don't have any file/directory name with "a/b/c" or any
    // sub-directory or file name from the given path we return this enum value.
    NONE
  }

  /**
   * Add entries to the Key Table cache.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyInfo
   * @param parentInfoList
   * @param index
   *
   * TODO : move code to a separate utility class.
   */
  public static void addKeyTableCacheEntries(
      OMMetadataManager omMetadataManager, String volumeName,
      String bucketName, Optional<OmKeyInfo> keyInfo,
      Optional<List<OmKeyInfo>> parentInfoList,
      long index) {
    for (OmKeyInfo parentInfo : parentInfoList.get()) {
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
              parentInfo.getKeyName())),
          new CacheValue<>(Optional.of(parentInfo), index));
    }

    if (keyInfo.isPresent()) {
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
                  keyInfo.get().getKeyName())),
          new CacheValue<>(keyInfo, index));
    }
  }

  public static void addDirectoryTableCacheEntries(
          OMMetadataManager omMetadataManager, String volumeName,
          String bucketName, Optional<OmDirectoryInfo> dirInfo,
          Optional<List<OmDirectoryInfo>> parentInfoList,
          long index) {
    for (OmDirectoryInfo parentInfo : parentInfoList.get()) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(omMetadataManager.getOzonePrefixKey(
                      parentInfo.getParentObjectID(), parentInfo.getName())),
              new CacheValue<>(Optional.of(parentInfo), index));
    }

    if (dirInfo.isPresent()) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(omMetadataManager.getOzonePrefixKey(dirInfo.get().getParentObjectID(),
                      dirInfo.get().getName())),
              new CacheValue<>(dirInfo, index));
    }
  }

  /**
   * Verify that the given toKey directory is a sub directory of fromKey
   * directory.
   * <p>
   * For example, rename a directory to its own subdirectory is not
   * allowed.
   *
   * @param fromKeyName
   * @param toKeyName
   * @return
   */
  public static boolean verifyToDirIsASubDirOfFromDirectory(String fromKeyName,
                                           String toKeyName, boolean isDir){
    if (!isDir) {
      return false;
    }
    Path dstParent = Paths.get(toKeyName).getParent();
    while (dstParent != null && !Paths.get(fromKeyName).equals(dstParent)) {
      dstParent = dstParent.getParent();
    }
    Preconditions.checkArgument(dstParent == null,
            "Cannot rename a directory to its own subdirectory");
    return false;
  }

  /**
   * Verify parent exists for the toKeyName.
   * <p>
   * For example, check whether dst parent dir exists or not
   * if the parent exists, the source can still be renamed to dst path.
   *
   * @param volumeName
   * @param toKeyName
   * @param bucketName
   * @param fromKeyName
   * @param metaMgr
   * @throws IOException if the destination parent dir doesn't exists.
   */
  public static void verifyToKeynameParentDirExists(String volumeName,
                                                    String bucketName,
                                                    String toKeyName,
                                                    String fromKeyName,
                                                    OMMetadataManager metaMgr)
          throws IOException {
    OmDirectoryInfo toDirInfo =
            OMFileRequest.getLastKnownPrefixIfExists(volumeName, bucketName,
                    toKeyName, metaMgr);
    int totalDirsCount = OzoneFSUtils.getFileCount(toKeyName);
    // skip parent is root '/'
    if(totalDirsCount <= 1){
      return;
    }
    // check if the immediate parent exists
    if (toDirInfo.getIndex() != totalDirsCount - 2) {
      Path toKeyParent = Paths.get(toKeyName).getParent();
      throw new IOException(String.format(
              "Failed to rename %s to %s, %s is a file", fromKeyName, toKeyName,
              toKeyParent));
    }
  }
}
