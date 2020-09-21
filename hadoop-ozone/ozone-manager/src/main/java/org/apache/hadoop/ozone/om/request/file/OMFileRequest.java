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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
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
    List<OzoneAcl> inheritAcls = new ArrayList<>();
    OMDirectoryResult result = OMDirectoryResult.NONE;

    Iterator<Path> elements = keyPath.iterator();
    // TODO: volume id and bucket id generation logic.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    inheritAcls = omBucketInfo.getAcls();
    long lastKnownParentId = omBucketInfo.getObjectID();
    OmDirectoryInfo parentDirInfo = null;
    String dbDirName = ""; // absolute path for trace logs
    // for better logging
    StringBuilder exisitingKeyPath = new StringBuilder(bucketKey);
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      exisitingKeyPath.append(OzoneConsts.OM_KEY_PREFIX);
      exisitingKeyPath.append(fileName);
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
      String dbNodeName = omMetadataManager.getOzoneLeafNodeKey(
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

        String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
                bucketName, dbDirName);
        LOG.trace("Acls inherited from parent " + dbDirKeyName + " are : "
                + inheritAcls);
      }
    }

    if (result == OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH) {
      String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
              bucketName, dbDirName);
      LOG.trace("Acls inherited from parent " + dbDirKeyName + " are : "
              + inheritAcls);
    }

    if (result != OMDirectoryResult.NONE) {
      LOG.trace("verifyFiles in Path : " + "/" + volumeName
              + "/" + bucketName + "/" + keyName + ":" + result);
      return new OMPathInfoV1(leafNodeName, lastKnownParentId, missing, result,
              inheritAcls, exisitingKeyPath.toString());
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
    return new OMPathInfoV1(leafNodeName, lastKnownParentId, missing,
            OMDirectoryResult.NONE, inheritAcls, exisitingKeyPath.toString());
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
   * Class to return the results from verifyDirectoryKeysInPath.
   * Includes the list of missing intermediate directories and
   * the directory search result code.
   */
  public static class OMPathInfoV1 {
    private OMDirectoryResult directoryResult;
    private String leafNodeName;
    private long lastKnownParentId;
    private long leafNodeObjectId;
    private List<String> missingParents;
    private List<OzoneAcl> acls;
    private String exisitingKeyPath;

    public OMPathInfoV1(String leafNodeName, long lastKnownParentId,
                        List missingParents, OMDirectoryResult result,
                        List<OzoneAcl> aclList, String exisitingPathName) {
      this.leafNodeName = leafNodeName;
      this.lastKnownParentId = lastKnownParentId;
      this.missingParents = missingParents;
      this.directoryResult = result;
      this.acls = aclList;
      this.exisitingKeyPath = exisitingPathName;
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

    public List getMissingParents() {
      return missingParents;
    }

    public OMDirectoryResult getDirectoryResult() {
      return directoryResult;
    }

    public List<OzoneAcl> getAcls() {
      return acls;
    }

    public String getExisitingKeyPath() {
      return exisitingKeyPath;
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
   * @param omMetadataManager  OM Metdata Manager
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
              new CacheKey<>(omMetadataManager.getOzoneLeafNodeKey(
                      subDirInfo.getParentObjectID(), subDirInfo.getName())),
              new CacheValue<>(Optional.of(subDirInfo), trxnLogIndex));
    }

    if (dirInfo.isPresent()) {
      omMetadataManager.getDirectoryTable().addCacheEntry(
              new CacheKey<>(omMetadataManager.getOzoneLeafNodeKey(
                      dirInfo.get().getParentObjectID(),
                      dirInfo.get().getName())),
              new CacheValue<>(dirInfo, trxnLogIndex));
    }
  }

}
