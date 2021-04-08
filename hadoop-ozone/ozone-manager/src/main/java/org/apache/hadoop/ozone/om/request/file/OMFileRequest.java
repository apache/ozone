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
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.jetbrains.annotations.NotNull;
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

  private OMFileRequest() {
  }
  /**
   * Verify any files exist in the given path in the specified volume/bucket.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @param keyPath
   * @return true - if file exist in the given path, else false.
   * @throws IOException
   */
  public static OMPathInfo verifyFilesInPath(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull String volumeName,
      @Nonnull String bucketName, @Nonnull String keyName,
      @Nonnull Path keyPath) throws IOException {

    String fileNameFromDetails = omMetadataManager.getOzoneKey(volumeName,
        bucketName, keyName);
    String dirNameFromDetails = omMetadataManager.getOzoneDirKey(volumeName,
        bucketName, keyName);
    List<String> missing = new ArrayList<>();
    List<OzoneAcl> inheritAcls = new ArrayList<>();
    OMDirectoryResult result = OMDirectoryResult.NONE;

    while (keyPath != null) {
      String pathName = keyPath.toString();

      String dbKeyName = omMetadataManager.getOzoneKey(volumeName,
          bucketName, pathName);
      String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
          bucketName, pathName);

      if (omMetadataManager.getKeyTable().isExist(dbKeyName)) {
        // Found a file in the given path.
        // Check if this is actual file or a file in the given path
        if (dbKeyName.equals(fileNameFromDetails)) {
          result = OMDirectoryResult.FILE_EXISTS;
        } else {
          result = OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
        }
      } else if (omMetadataManager.getKeyTable().isExist(dbDirKeyName)) {
        // Found a directory in the given path.
        // Check if this is actual directory or a directory in the given path
        if (dbDirKeyName.equals(dirNameFromDetails)) {
          result = OMDirectoryResult.DIRECTORY_EXISTS;
        } else {
          result = OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
          inheritAcls = omMetadataManager.getKeyTable().get(dbDirKeyName)
              .getAcls();
          LOG.trace("Acls inherited from parent " + dbDirKeyName + " are : "
              + inheritAcls);
        }
      } else {
        if (!dbDirKeyName.equals(dirNameFromDetails)) {
          missing.add(keyPath.toString());
        }
      }

      if (result != OMDirectoryResult.NONE) {

        LOG.trace("verifyFiles in Path : " + "/" + volumeName
            + "/" + bucketName + "/" + keyName + ":" + result);
        return new OMPathInfo(missing, result, inheritAcls);
      }
      keyPath = keyPath.getParent();
    }

    if (inheritAcls.isEmpty()) {
      String bucketKey = omMetadataManager.getBucketKey(volumeName,
          bucketName);
      inheritAcls = omMetadataManager.getBucketTable().get(bucketKey)
          .getAcls();
      LOG.trace("Acls inherited from bucket " + bucketName + " are : "
          + inheritAcls);
    }

    LOG.trace("verifyFiles in Path : " + volumeName + "/" + bucketName + "/"
        + keyName + ":" + result);
    // Found no files/ directories in the given path.
    return new OMPathInfo(missing, OMDirectoryResult.NONE, inheritAcls);
  }

  /**
   * Verify any dir/key exist in the given path in the specified
   * volume/bucket by iterating through directory table.
   *
   * @param omMetadataManager OM Metadata manager
   * @param volumeName        volume name
   * @param bucketName        bucket name
   * @param keyName           key name
   * @param keyPath           path
   * @return OMPathInfoV1 path info object
   * @throws IOException on DB failure
   */
  public static OMPathInfoV1 verifyDirectoryKeysInPath(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull String volumeName,
          @Nonnull String bucketName, @Nonnull String keyName,
          @Nonnull Path keyPath) throws IOException {

    String leafNodeName = OzoneFSUtils.getFileName(keyName);
    List<String> missing = new ArrayList<>();

    // Found no files/ directories in the given path.
    OMDirectoryResult result = OMDirectoryResult.NONE;

    Iterator<Path> elements = keyPath.iterator();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    // by default, inherit bucket ACLs
    List<OzoneAcl> inheritAcls = omBucketInfo.getAcls();

    long lastKnownParentId = omBucketInfo.getObjectID();
    String dbDirName = ""; // absolute path for trace logs
    // for better logging
    StringBuilder fullKeyPath = new StringBuilder(bucketKey);
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      fullKeyPath.append(OzoneConsts.OM_KEY_PREFIX);
      fullKeyPath.append(fileName);
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
      String dbNodeName = omMetadataManager.getOzonePathKey(
              lastKnownParentId, fileName);
      OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().
              get(dbNodeName);
      if (omDirInfo != null) {
        dbDirName += omDirInfo.getName() + OzoneConsts.OZONE_URI_DELIMITER;
        if (elements.hasNext()) {
          result = OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
          lastKnownParentId = omDirInfo.getObjectID();
          inheritAcls = omDirInfo.getAcls();
          continue;
        } else {
          // Checked all the sub-dirs till the leaf node.
          // Found a directory in the given path.
          result = OMDirectoryResult.DIRECTORY_EXISTS;
        }
      } else {
        // Get parentID from the lastKnownParent. For any files, directly under
        // the bucket, the parent is the bucketID. Say, "/vol1/buck1/file1"
        // TODO: Need to add UT for this case along with OMFileCreateRequest.
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

        // Add to missing list, there is no such file/directory with given name.
        if (elements.hasNext()) {
          missing.add(fileName);
        }
      }
    }

    LOG.trace("verifyFiles/Directories in Path : " + "/" + volumeName
            + "/" + bucketName + "/" + keyName + ":" + result);

    if (result == OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH || result ==
            OMDirectoryResult.FILE_EXISTS) {
      return new OMPathInfoV1(leafNodeName, lastKnownParentId, missing,
              result, inheritAcls, fullKeyPath.toString());
    }

    String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
            bucketName, dbDirName);
    LOG.trace("Acls inherited from parent " + dbDirKeyName + " are : "
            + inheritAcls);

    return new OMPathInfoV1(leafNodeName, lastKnownParentId, missing,
            result, inheritAcls);
  }

  /**
   * Class to return the results from verifyDirectoryKeysInPath.
   * Includes the list of missing intermediate directories and
   * the directory search result code.
   */
  public static class OMPathInfoV1 extends OMPathInfo{
    private String leafNodeName;
    private long lastKnownParentId;
    private long leafNodeObjectId;
    private String fileExistsInPath;

    public OMPathInfoV1(String leafNodeName, long lastKnownParentId,
                        List missingParents, OMDirectoryResult result,
                        List<OzoneAcl> aclList, String fileExistsInPath) {
      super(missingParents, result, aclList);
      this.leafNodeName = leafNodeName;
      this.lastKnownParentId = lastKnownParentId;
      this.fileExistsInPath = fileExistsInPath;
    }

    public OMPathInfoV1(String leafNodeName, long lastKnownParentId,
                        List missingParents, OMDirectoryResult result,
                        List<OzoneAcl> aclList) {
      this(leafNodeName, lastKnownParentId, missingParents, result, aclList,
              "");
    }

    public String getLeafNodeName() {
      return leafNodeName;
    }

    public long getLeafNodeObjectId() {
      return leafNodeObjectId;
    }

    public void setLeafNodeObjectId(long leafNodeObjectId) {
      this.leafNodeObjectId = leafNodeObjectId;
    }

    public void setLastKnownParentId(long lastKnownParentId) {
      this.lastKnownParentId = lastKnownParentId;
    }

    public long getLastKnownParentId() {
      return lastKnownParentId;
    }

    public String getFileExistsInPath() {
      return fileExistsInPath;
    }
  }

  /**
   * Class to return the results from verifyFilesInPath.
   * Includes the list of missing intermediate directories and
   * the directory search result code.
   */
  public static class OMPathInfo {
    private OMDirectoryResult directoryResult;
    private List<String> missingParents;
    private List<OzoneAcl> acls;

    public OMPathInfo(List missingParents, OMDirectoryResult result,
        List<OzoneAcl> aclList) {
      this.missingParents = missingParents;
      this.directoryResult = result;
      this.acls = aclList;
    }

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
  public enum OMDirectoryResult {

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

  /**
   * Adding directory info to the Table cache.
   *
   * @param omMetadataManager  OM Metadata Manager
   * @param dirInfo            directory info
   * @param missingParentInfos list of the parents to be added to DB
   * @param trxnLogIndex       transaction log index
   */
  public static void addDirectoryTableCacheEntries(
          OMMetadataManager omMetadataManager,
          Optional<OmDirectoryInfo> dirInfo,
          Optional<List<OmDirectoryInfo>> missingParentInfos,
          long trxnLogIndex) {
    for (OmDirectoryInfo subDirInfo : missingParentInfos.get()) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(omMetadataManager.getOzonePathKey(
                      subDirInfo.getParentObjectID(), subDirInfo.getName())),
              new CacheValue<>(Optional.of(subDirInfo), trxnLogIndex));
    }

    if (dirInfo.isPresent()) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(omMetadataManager.getOzonePathKey(
                      dirInfo.get().getParentObjectID(),
                      dirInfo.get().getName())),
              new CacheValue<>(dirInfo, trxnLogIndex));
    }
  }

  /**
   * Adding Key info to the openFile Table cache.
   *
   * @param omMetadataManager OM Metadata Manager
   * @param dbOpenFileName    open file name key
   * @param omFileInfo        key info
   * @param fileName          file name
   * @param trxnLogIndex      transaction log index
   */
  public static void addOpenFileTableCacheEntry(
          OMMetadataManager omMetadataManager, String dbOpenFileName,
          @Nullable OmKeyInfo omFileInfo, String fileName, long trxnLogIndex) {

    Optional<OmKeyInfo> keyInfoOptional = Optional.absent();
    if (omFileInfo != null) {
      // New key format for the openFileTable.
      // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
      // keyName field stores only the leaf node name, which is 'file1'.
      omFileInfo.setKeyName(fileName);
      keyInfoOptional = Optional.of(omFileInfo);
    }

    omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(dbOpenFileName),
            new CacheValue<>(keyInfoOptional, trxnLogIndex));
  }

  /**
   * Adding Key info to the file table cache.
   *
   * @param omMetadataManager OM Metadata Manager
   * @param dbFileKey         file name key
   * @param omFileInfo        key info
   * @param fileName          file name
   * @param trxnLogIndex      transaction log index
   */
  public static void addFileTableCacheEntry(
          OMMetadataManager omMetadataManager, String dbFileKey,
          OmKeyInfo omFileInfo, String fileName, long trxnLogIndex) {

    // New key format for the fileTable.
    // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
    // keyName field stores only the leaf node name, which is 'file1'.
    omFileInfo.setKeyName(fileName);

    omMetadataManager.getKeyTable().addCacheEntry(
            new CacheKey<>(dbFileKey),
            new CacheValue<>(Optional.of(omFileInfo), trxnLogIndex));
  }

  /**
   * Adding omKeyInfo to open file table.
   *
   * @param omMetadataMgr    OM Metadata Manager
   * @param batchOp          batch of db operations
   * @param omFileInfo       omKeyInfo
   * @param openKeySessionID clientID
   * @throws IOException DB failure
   */
  public static void addToOpenFileTable(OMMetadataManager omMetadataMgr,
                                        BatchOperation batchOp,
                                        OmKeyInfo omFileInfo,
                                        long openKeySessionID)
          throws IOException {

    String dbOpenFileKey = omMetadataMgr.getOpenFileName(
            omFileInfo.getParentObjectID(), omFileInfo.getFileName(),
            openKeySessionID);

    omMetadataMgr.getOpenKeyTable().putWithBatch(batchOp, dbOpenFileKey,
            omFileInfo);
  }

  /**
   * Adding omKeyInfo to file table.
   *
   * @param omMetadataMgr
   * @param batchOp
   * @param omFileInfo
   * @throws IOException
   */
  public static void addToFileTable(OMMetadataManager omMetadataMgr,
                                    BatchOperation batchOp,
                                    OmKeyInfo omFileInfo)
          throws IOException {

    String dbFileKey = omMetadataMgr.getOzonePathKey(
            omFileInfo.getParentObjectID(), omFileInfo.getFileName());

    omMetadataMgr.getKeyTable().putWithBatch(batchOp,
            dbFileKey, omFileInfo);
  }

  /**
   * Gets om key info from open key table if openFileTable flag is true,
   * otherwise get it from key table.
   *
   * @param openFileTable if true add KeyInfo to openFileTable, otherwise to
   *                      fileTable
   * @param omMetadataMgr OM Metadata Manager
   * @param dbOpenFileKey open file kaye name in DB
   * @param keyName       key name
   * @return om key info
   * @throws IOException DB failure
   */
  public static OmKeyInfo getOmKeyInfoFromFileTable(boolean openFileTable,
      OMMetadataManager omMetadataMgr, String dbOpenFileKey, String keyName)
          throws IOException {

    OmKeyInfo dbOmKeyInfo;
    if (openFileTable) {
      dbOmKeyInfo = omMetadataMgr.getOpenKeyTable().get(dbOpenFileKey);
    } else {
      dbOmKeyInfo = omMetadataMgr.getKeyTable().get(dbOpenFileKey);
    }

    // DB OMKeyInfo will store only fileName into keyName field. This
    // function is to set user given keyName into the OmKeyInfo object.
    // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
    // keyName field stores only the leaf node name, which is 'file1'.
    if (dbOmKeyInfo != null) {
      dbOmKeyInfo.setKeyName(keyName);
    }
    return dbOmKeyInfo;
  }

  /**
   * Gets OmKeyInfo if exists for the given key name in the DB.
   *
   * @param omMetadataMgr metadata manager
   * @param volumeName    volume name
   * @param bucketName    bucket name
   * @param keyName       key name
   * @param scmBlockSize  scm block size
   * @return OzoneFileStatus
   * @throws IOException DB failure
   */
  @Nullable
  public static OzoneFileStatus getOMKeyInfoIfExists(
      OMMetadataManager omMetadataMgr, String volumeName, String bucketName,
      String keyName, long scmBlockSize) throws IOException {

    Path keyPath = Paths.get(keyName);
    Iterator<Path> elements = keyPath.iterator();
    String bucketKey = omMetadataMgr.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataMgr.getBucketTable().get(bucketKey);

    long lastKnownParentId = omBucketInfo.getObjectID();
    OmDirectoryInfo omDirInfo = null;
    while (elements.hasNext()) {
      String fileName = elements.next().toString();

      // For example, /vol1/buck1/a/b/c/d/e/file1.txt
      // 1. Do lookup path component on directoryTable starting from bucket
      // 'buck1' to the leaf node component, which is 'file1.txt'.
      // 2. If there is no dir exists for the leaf node component 'file1.txt'
      // then do look it on fileTable.
      String dbNodeName = omMetadataMgr.getOzonePathKey(
              lastKnownParentId, fileName);
      omDirInfo = omMetadataMgr.getDirectoryTable().get(dbNodeName);

      if (omDirInfo != null) {
        lastKnownParentId = omDirInfo.getObjectID();
      } else if (!elements.hasNext()) {
        // reached last path component. Check file exists for the given path.
        OmKeyInfo omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(false,
                omMetadataMgr, dbNodeName, keyName);
        if (omKeyInfo != null) {
          return new OzoneFileStatus(omKeyInfo, scmBlockSize, false);
        }
      } else {
        // Missing intermediate directory and just return null;
        // key not found in DB
        return null;
      }
    }

    if (omDirInfo != null) {
      OmKeyInfo omKeyInfo = getOmKeyInfo(volumeName, bucketName, omDirInfo,
              keyName);
      return new OzoneFileStatus(omKeyInfo, scmBlockSize, true);
    }

    // key not found in DB
    return null;
  }

  /**
   * Prepare OmKeyInfo from OmDirectoryInfo.
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param dirInfo    directory info
   * @param keyName    user given key name
   * @return OmKeyInfo object
   */
  @NotNull
  public static OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
      OmDirectoryInfo dirInfo, String keyName) {

    OmKeyInfo.Builder builder = new OmKeyInfo.Builder();
    builder.setParentObjectID(dirInfo.getParentObjectID());
    builder.setKeyName(keyName);
    builder.setAcls(dirInfo.getAcls());
    builder.addAllMetadata(dirInfo.getMetadata());
    builder.setVolumeName(volumeName);
    builder.setBucketName(bucketName);
    builder.setCreationTime(dirInfo.getCreationTime());
    builder.setModificationTime(dirInfo.getModificationTime());
    builder.setObjectID(dirInfo.getObjectID());
    builder.setUpdateID(dirInfo.getUpdateID());
    builder.setFileName(dirInfo.getName());
    builder.setReplicationType(HddsProtos.ReplicationType.RATIS);
    builder.setReplicationFactor(HddsProtos.ReplicationFactor.ONE);
    builder.setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())));
    return builder.build();
  }

  /**
   * Returns absolute path.
   *
   * @param prefixName prefix path
   * @param fileName   file name
   * @return absolute path
   */
  @NotNull
  public static String getAbsolutePath(String prefixName, String fileName) {
    if (Strings.isNullOrEmpty(prefixName)) {
      return fileName;
    }
    return prefixName.concat(OzoneConsts.OZONE_URI_DELIMITER).concat(fileName);
  }

  /**
   * Build DirectoryInfo from OmKeyInfo.
   *
   * @param keyInfo omKeyInfo
   * @return omDirectoryInfo object
   */
  public static OmDirectoryInfo getDirectoryInfo(OmKeyInfo keyInfo){
    OmDirectoryInfo.Builder builder = new OmDirectoryInfo.Builder();
    builder.setParentObjectID(keyInfo.getParentObjectID());
    builder.setAcls(keyInfo.getAcls());
    builder.addAllMetadata(keyInfo.getMetadata());
    builder.setCreationTime(keyInfo.getCreationTime());
    builder.setModificationTime(keyInfo.getModificationTime());
    builder.setObjectID(keyInfo.getObjectID());
    builder.setUpdateID(keyInfo.getUpdateID());
    builder.setName(OzoneFSUtils.getFileName(keyInfo.getKeyName()));
    return builder.build();
  }

  /**
   * Verify that the given toKey directory is a sub directory of fromKey
   * directory.
   * <p>
   * For example, special case of renaming a directory to its own
   * sub-directory is not allowed.
   *
   * @param fromKeyName source path
   * @param toKeyName   destination path
   * @param isDir       true represents a directory type otw a file type
   * @throws OMException if the dest dir is a sub-dir of source dir.
   */
  public static void verifyToDirIsASubDirOfFromDirectory(String fromKeyName,
      String toKeyName, boolean isDir) throws OMException {
    if (!isDir) {
      return;
    }
    Path dstParent = Paths.get(toKeyName).getParent();
    while (dstParent != null) {
      if (Paths.get(fromKeyName).equals(dstParent)) {
        throw new OMException("Cannot rename a directory to its own " +
                "subdirectory", OMException.ResultCodes.KEY_RENAME_ERROR);
        // TODO: Existing rename throws java.lang.IllegalArgumentException.
        //       Should we throw same exception ?
      }
      dstParent = dstParent.getParent();
    }
  }

  /**
   * Verify parent exists for the destination path and return destination
   * path parent Id.
   * <p>
   * Check whether dst parent dir exists or not. If the parent exists, then the
   * source can be renamed to dst path.
   *
   * @param volumeName  volume name
   * @param bucketName  bucket name
   * @param toKeyName   destination path
   * @param fromKeyName source path
   * @param metaMgr     metadata manager
   * @throws IOException if the destination parent dir doesn't exists.
   */
  public static long getToKeyNameParentId(String volumeName,
      String bucketName, String toKeyName, String fromKeyName,
      OMMetadataManager metaMgr) throws IOException {

    int totalDirsCount = OzoneFSUtils.getFileCount(toKeyName);
    // skip parent is root '/'
    if (totalDirsCount <= 1) {
      String bucketKey = metaMgr.getBucketKey(volumeName, bucketName);
      OmBucketInfo omBucketInfo =
              metaMgr.getBucketTable().get(bucketKey);
      return omBucketInfo.getObjectID();
    }

    String toKeyParentDir = OzoneFSUtils.getParentDir(toKeyName);

    OzoneFileStatus toKeyParentDirStatus = getOMKeyInfoIfExists(metaMgr,
            volumeName, bucketName, toKeyParentDir, 0);
    // check if the immediate parent exists
    if (toKeyParentDirStatus == null) {
      throw new OMException(String.format(
              "Failed to rename %s to %s, %s doesn't exist", fromKeyName,
              toKeyName, toKeyParentDir),
              OMException.ResultCodes.KEY_RENAME_ERROR);
    } else if (toKeyParentDirStatus.isFile()){
      throw new OMException(String.format(
              "Failed to rename %s to %s, %s is a file", fromKeyName, toKeyName,
              toKeyParentDir), OMException.ResultCodes.KEY_RENAME_ERROR);
    }
    return toKeyParentDirStatus.getKeyInfo().getObjectID();
  }
}
