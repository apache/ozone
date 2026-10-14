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

package org.apache.hadoop.ozone.om.request.file;

import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

import com.google.common.base.Strings;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValueIterator;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.snapshot.diff.SnapshotDiffValueParser;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsLeaseInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for file requests.
 */
public final class OMFileRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMFileRequest.class);

  static List<String> getKeyPathComponents(String keyName) {
    String[] components = StringUtils.split(keyName, PATH_SEPARATOR_STR);
    return components == null ? new ArrayList<>() : Arrays.asList(components);
  }

  /**
   * Resolve a live native FSO identity. The caller must hold the bucket read lock through authorization and lookup.
   */
  public static String getNfsObjectPath(OMMetadataManager manager, String volumeName, String bucketName,
      long volumeId, long bucketId, long objectId, NfsFileHandle.Kind kind) throws IOException {
    if (kind == null) {
      throw new OMException("Missing NFS object kind", OMException.ResultCodes.INVALID_REQUEST);
    }
    if (!manager.isNfsObjectIndexEnabled()) {
      throw new OMException("NFS object index layout is not finalized",
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
    if (!manager.isNfsObjectIndexReady()) {
      throw new OMException("NFS object index bootstrap is incomplete", OMException.ResultCodes.OM_NOT_INITIALIZED);
    }
    OmVolumeArgs volume = manager.getVolumeTable().get(manager.getVolumeKey(volumeName));
    OmBucketInfo bucket = manager.getBucketTable().get(manager.getBucketKey(volumeName, bucketName));
    if (volume == null || bucket == null || volume.getObjectID() != volumeId || bucket.getObjectID() != bucketId) {
      throw new OMException("NFS volume or bucket incarnation is stale", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    if (bucket.isLink() || bucket.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      throw new OMException("NFS requires a native FSO bucket", OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
    if (kind == NfsFileHandle.Kind.BUCKET) {
      if (objectId != bucketId) {
        throw new OMException("NFS bucket identity is stale", OMException.ResultCodes.FILE_NOT_FOUND);
      }
      return "";
    }
    if (objectId == bucketId) {
      throw new OMException("NFS object kind does not match bucket root", OMException.ResultCodes.FILE_NOT_FOUND);
    }
    PersistedNfsObjectInfo.Kind expected = kind == NfsFileHandle.Kind.FILE
        ? PersistedNfsObjectInfo.Kind.FILE : PersistedNfsObjectInfo.Kind.DIRECTORY;
    Set<Long> visited = new HashSet<>();
    Deque<String> components = new ArrayDeque<>();
    long current = objectId;
    while (current != bucketId) {
      OmNfsObjectInfo pointer = manager.getNfsObjectTable().get(manager.getNfsObjectKey(volumeId, bucketId, current));
      if (!visited.add(current) || pointer == null || pointer.getKind() != expected) {
        throw new OMException("NFS object or ancestor identity is stale", OMException.ResultCodes.FILE_NOT_FOUND);
      }
      String key = manager.getOzonePathKey(volumeId, bucketId, pointer.getParentObjectID(), pointer.getName());
      boolean matches;
      if (expected == PersistedNfsObjectInfo.Kind.DIRECTORY) {
        OmDirectoryInfo directory = manager.getDirectoryTable().get(key);
        matches = directory != null && directory.getObjectID() == current
            && directory.getParentObjectID() == pointer.getParentObjectID()
            && directory.getName().equals(pointer.getName());
      } else {
        OmKeyInfo file = manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).get(key);
        matches = file != null && file.getObjectID() == current
            && file.getParentObjectID() == pointer.getParentObjectID()
            && file.getFileName().equals(pointer.getName()) && volumeName.equals(file.getVolumeName())
            && bucketName.equals(file.getBucketName());
      }
      if (!matches) {
        throw new OMException("NFS index does not match canonical identity", OMException.ResultCodes.FILE_NOT_FOUND);
      }
      components.addFirst(pointer.getName());
      current = pointer.getParentObjectID();
      expected = PersistedNfsObjectInfo.Kind.DIRECTORY;
    }
    String path = String.join("/", components);
    if (path.equals(OzoneConsts.OM_SNAPSHOT_INDICATOR)
        || path.startsWith(OzoneConsts.OM_SNAPSHOT_INDICATOR + "/")) {
      throw new OMException("NFS snapshot paths are unsupported", OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
    return path;
  }

  /** Reject a mutation that would invalidate a file with a live NFS writer. */
  public static void validateNfsFileLeaseInactive(OMMetadataManager manager, OmKeyInfo keyInfo)
      throws IOException {
    long volumeId = manager.getVolumeId(keyInfo.getVolumeName());
    long bucketId = manager.getBucketId(keyInfo.getVolumeName(), keyInfo.getBucketName());
    String leaseKey = manager.getNfsFileLeaseKey(volumeId, bucketId, keyInfo.getObjectID());
    OmNfsLeaseInfo lease = manager.getNfsFileLeaseTable().get(leaseKey);
    if (lease != null && lease.getProtobuf().getExpiresAtMillis() > Time.now()) {
      throw new OMException("File has an active NFS writer lease", OMException.ResultCodes.NFS_LEASE_CONFLICT);
    }
  }

  /** Reject a directory mutation while any descendant file has a live NFS writer lease. */
  public static void validateNfsDirectoryLeasesInactive(OMMetadataManager manager, long volumeId, long bucketId,
      long directoryObjectId) throws IOException {
    if (!manager.isNfsObjectIndexEnabled() || !manager.isNfsObjectIndexReady()) {
      return;
    }
    String objectPrefix = manager.getNfsObjectKey(volumeId, bucketId, 0);
    objectPrefix = objectPrefix.substring(0, objectPrefix.lastIndexOf('/') + 1);
    Map<Long, OmNfsLeaseInfo> leases = new HashMap<>();
    Table<String, OmNfsLeaseInfo> leaseTable = manager.getNfsFileLeaseTable();
    try (KeyValueIterator<String, OmNfsLeaseInfo> iterator = leaseTable.iterator(objectPrefix)) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmNfsLeaseInfo> entry = iterator.next();
        if (entry != null) {
          OmNfsLeaseInfo lease = entry.getValue();
          leases.put(lease.getProtobuf().getObjectID(), lease);
        }
      }
    }
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmNfsLeaseInfo>>> cacheIterator = leaseTable.cacheIterator();
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmNfsLeaseInfo>> entry = cacheIterator.next();
      String key = entry.getKey().getCacheKey();
      if (!key.startsWith(objectPrefix)) {
        continue;
      }
      OmNfsLeaseInfo lease = entry.getValue().getCacheValue();
      long objectId = parseNfsObjectId(key);
      if (lease == null) {
        leases.remove(objectId);
      } else {
        leases.put(objectId, lease);
      }
    }
    long now = Time.now();
    for (OmNfsLeaseInfo lease : leases.values()) {
      if (lease.getProtobuf().getKind() == PersistedNfsLeaseInfo.Kind.FILE
          && lease.getProtobuf().getExpiresAtMillis() > now
          && isNfsDescendant(manager, volumeId, bucketId, lease.getProtobuf().getObjectID(), directoryObjectId)) {
        throw new OMException("Directory contains a file with an active NFS writer lease",
            OMException.ResultCodes.NFS_LEASE_CONFLICT);
      }
    }
  }

  private static long parseNfsObjectId(String key) {
    return Long.parseLong(key.substring(key.lastIndexOf('/') + 1));
  }

  private static boolean isNfsDescendant(OMMetadataManager manager, long volumeId, long bucketId, long objectId,
      long directoryObjectId) throws IOException {
    Set<Long> visited = new HashSet<>();
    long current = objectId;
    while (current != bucketId) {
      if (!visited.add(current)) {
        throw new OMException("NFS object index contains a parent cycle", OMException.ResultCodes.NFS_LEASE_CONFLICT);
      }
      OmNfsObjectInfo info = manager.getNfsObjectTable().get(manager.getNfsObjectKey(volumeId, bucketId, current));
      if (info == null) {
        throw new OMException("NFS object index is incomplete while checking writer leases",
            OMException.ResultCodes.NFS_LEASE_CONFLICT);
      }
      if (info.getParentObjectID() == directoryObjectId) {
        return true;
      }
      current = info.getParentObjectID();
    }
    return false;
  }

  private OMFileRequest() {
  }

  /**
   * Verify any files exist in the given path in the specified volume/bucket.
   *
   * @param omMetadataManager OMMetadataManager
   * @param volumeName        Volume Name
   * @param bucketName        Bucket Name
   * @param keyPath           Key Path
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
    // Get parent all acls including ACCESS and DEFAULT acls
    // The logic of specific inherited acl should be when creating dir/file
    List<OzoneAcl> acls = new ArrayList<>();
    OMDirectoryResult result = OMDirectoryResult.NONE;

    while (keyPath != null) {
      String pathName = keyPath.toString();

      String dbKeyName = omMetadataManager.getOzoneKey(volumeName,
          bucketName, pathName);
      String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
          bucketName, pathName);

      if (omMetadataManager.getKeyTable(
          getBucketLayout(omMetadataManager, volumeName, bucketName))
          .isExist(dbKeyName)) {
        // Found a file in the given path.
        // Check if this is actual file or a file in the given path
        if (dbKeyName.equals(fileNameFromDetails)) {
          result = OMDirectoryResult.FILE_EXISTS;
        } else {
          result = OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
        }
      } else if (omMetadataManager.getKeyTable(
          getBucketLayout(omMetadataManager, volumeName, bucketName))
          .isExist(dbDirKeyName)) {
        // Found a directory in the given path.
        // Check if this is actual directory or a directory in the given path
        if (dbDirKeyName.equals(dirNameFromDetails)) {
          result = OMDirectoryResult.DIRECTORY_EXISTS;
        } else {
          result = OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
          acls = omMetadataManager.getKeyTable(
              getBucketLayout(omMetadataManager, volumeName, bucketName))
              .get(dbDirKeyName).getAcls();
          LOG.trace("Acls from parent {} are : {}", dbDirKeyName, acls);
        }
      } else {
        if (!dbDirKeyName.equals(dirNameFromDetails)) {
          missing.add(keyPath.toString());
        }
      }

      if (result != OMDirectoryResult.NONE) {

        LOG.trace("verifyFiles in Path : /{}/{}/{} : {}",
            volumeName, bucketName, keyName, result);
        return new OMPathInfo(missing, result, acls);
      }
      keyPath = keyPath.getParent();
    }

    if (acls.isEmpty()) {
      String bucketKey = omMetadataManager.getBucketKey(volumeName,
          bucketName);
      acls = omMetadataManager.getBucketTable().get(bucketKey)
          .getAcls();
      LOG.trace("Acls from bucket {} are : {}", bucketName, acls);
    }

    LOG.trace("verifyFiles in Path : /{}/{}/{} : {}",
        volumeName, bucketName, keyName, result);
    // Found no files/ directories in the given path.
    return new OMPathInfo(missing, OMDirectoryResult.NONE, acls);
  }

  /**
   * Reads only the objectID of the directory stored under the given directory table key, without
   * materializing an {@link OmDirectoryInfo}. The path walks below need each parent directory's
   * objectID, but a full read also decodes and rebuilds the ACL list, the metadata map and the
   * timestamps, which they discard.
   *
   * @return the directory's objectID, or null if there is no directory under that key.
   */
  private static Long getDirObjectId(OMMetadataManager omMetadataManager, String dbNodeName)
      throws IOException {
    return omMetadataManager.getDirectoryTable().getProjected(dbNodeName,
        OmDirectoryInfo::getObjectID,
        SnapshotDiffValueParser::parseDirectoryInfoObjectId);
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
   * @return OMPathInfoWithFSO path info object
   * @throws IOException on DB failure
   */
  public static OMPathInfoWithFSO verifyDirectoryKeysInPath(
          @Nonnull OMMetadataManager omMetadataManager,
          @Nonnull String volumeName,
          @Nonnull String bucketName, @Nonnull String keyName,
          @Nonnull Path keyPath) throws IOException {

    List<String> components = new ArrayList<>();
    keyPath.forEach(component -> components.add(component.toString()));
    return verifyDirectoryKeysInPath(omMetadataManager, volumeName, bucketName, keyName, components);
  }

  /** Verify an Ozone key name without interpreting it as a local filesystem path. */
  public static OMPathInfoWithFSO verifyDirectoryKeysInPath(
      @Nonnull OMMetadataManager omMetadataManager, @Nonnull String volumeName,
      @Nonnull String bucketName, @Nonnull String keyName) throws IOException {
    return verifyDirectoryKeysInPath(omMetadataManager, volumeName, bucketName, keyName,
        getKeyPathComponents(keyName));
  }

  private static OMPathInfoWithFSO verifyDirectoryKeysInPath(
      OMMetadataManager omMetadataManager, String volumeName, String bucketName,
      String keyName, List<String> keyPathComponents) throws IOException {

    String leafNodeName = keyPathComponents.isEmpty() ? keyName
        : keyPathComponents.get(keyPathComponents.size() - 1);
    List<String> missing = new ArrayList<>();

    // Found no files/ directories in the given path.
    OMDirectoryResult result = OMDirectoryResult.NONE;

    Iterator<String> elements = keyPathComponents.iterator();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    // Get parent all acls including ACCESS and DEFAULT acls
    // The logic of specific inherited acl should be when creating dir/file
    List<OzoneAcl> acls = omBucketInfo.getAcls();
    // Directory table key of the deepest existing directory on the path, i.e. the one the ACLs above
    // are replaced with below; null means the path has no such directory and the bucket ACLs apply.
    String aclParentDbKey = null;

    long lastKnownParentId = omBucketInfo.getObjectID();
    StringBuilder dbDirName = new StringBuilder(); // absolute path for trace logs
    // for better logging
    StringBuilder fullKeyPath = new StringBuilder(bucketKey);
    while (elements.hasNext()) {
      String fileName = elements.next();
      fullKeyPath.append(OzoneConsts.OM_KEY_PREFIX)
          .append(fileName);
      if (!missing.isEmpty()) {
        // Add all the sub-dirs to the missing list except the leaf element.
        // For example, /vol1/buck1/a/b/c/d/e/f/file1.txt.
        // Assume /vol1/buck1/a/b/c exists, then add d, e, f into missing list.
        if (elements.hasNext()) {
          // skips leaf node.
          missing.add(fileName);
        }
        continue;
      }

      // For example, /vol1/buck1/a/b/c/d/e/f/file1.txt
      // 1. Do lookup on directoryTable. If not exists goto next step.
      // 2. Do look on keyTable. If not exists goto next step.
      // 3. Add 'sub-dir' to missing parents list
      String dbNodeName = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              lastKnownParentId, fileName);
      final Long dirObjectId = getDirObjectId(omMetadataManager, dbNodeName);
      if (dirObjectId != null) {
        // dbNodeName ends with the directory's name, so fileName is what the stored name field holds.
        dbDirName.append(fileName).append(OzoneConsts.OZONE_URI_DELIMITER);
        if (elements.hasNext()) {
          result = OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
          lastKnownParentId = dirObjectId;
          aclParentDbKey = dbNodeName;
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
        if (omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
            .isExist(dbNodeName)) {
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

    LOG.trace("verifyFiles/Directories in Path : /{}/{}/{} : {}",
        volumeName, bucketName, keyName, result);

    // Inherited ACLs come from the deepest existing directory on the path, so they are read once here
    // instead of on every segment of the walk above.
    if (aclParentDbKey != null) {
      OmDirectoryInfo aclParent = omMetadataManager.getDirectoryTable().get(aclParentDbKey);
      if (aclParent != null) {
        acls = aclParent.getAcls();
      }
    }

    if (result == OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH || result ==
            OMDirectoryResult.FILE_EXISTS) {
      return new OMPathInfoWithFSO(leafNodeName, lastKnownParentId, missing,
              result, acls, fullKeyPath.toString());
    }

    String dbDirKeyName = omMetadataManager.getOzoneDirKey(volumeName,
        bucketName, dbDirName.toString());
    LOG.trace("Acls from parent {} are : {}", dbDirKeyName, acls);

    return new OMPathInfoWithFSO(leafNodeName, lastKnownParentId, missing,
            result, acls);
  }

  /**
   * Class to return the results from verifyDirectoryKeysInPath.
   * Includes the list of missing intermediate directories and
   * the directory search result code.
   */
  public static class OMPathInfoWithFSO extends OMPathInfo {
    private final String leafNodeName;
    private final String fileExistsInPath;
    private long lastKnownParentId;
    private long leafNodeObjectId;

    public OMPathInfoWithFSO(String leafNodeName, long lastKnownParentId,
                        List missingParents, OMDirectoryResult result,
                        List<OzoneAcl> aclList, String fileExistsInPath) {
      super(missingParents, result, aclList);
      this.leafNodeName = leafNodeName;
      this.lastKnownParentId = lastKnownParentId;
      this.fileExistsInPath = fileExistsInPath;
    }

    public OMPathInfoWithFSO(String leafNodeName, long lastKnownParentId,
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
    private final OMDirectoryResult directoryResult;
    private final List<String> missingParents;
    private final List<OzoneAcl> acls;

    public OMPathInfo(List missingParents, OMDirectoryResult result,
        List<OzoneAcl> aclList) {
      this.missingParents = missingParents;
      this.directoryResult = result;
      this.acls = aclList;
    }

    public List<String> getMissingParents() {
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
      String bucketName, BucketLayout layout,
      OmKeyInfo keyInfo, List<OmKeyInfo> parentInfoList,
      long index) throws IOException {
    final Table<String, OmKeyInfo> table
        = omMetadataManager.getKeyTable(layout);
    for (OmKeyInfo parentInfo : parentInfoList) {
      table.addCacheEntry(omMetadataManager.getOzoneKey(
          volumeName, bucketName, parentInfo.getKeyName()),
          parentInfo, index);
    }

    if (keyInfo != null) {
      table.addCacheEntry(omMetadataManager.getOzoneKey(
          volumeName, bucketName, keyInfo.getKeyName()),
          keyInfo, index);
    }
  }

  /**
   * Adding directory info to the Table cache.
   *
   * @param omMetadataManager  OM Metadata Manager
   * @param volumeId           ID of the Volume
   * @param bucketId           ID of the Bucket
   * @param trxnLogIndex       transaction log index
   * @param missingParentInfos list of the parents to be added to DB
   * @param dirInfo            directory info
   */
  public static void addDirectoryTableCacheEntries(
          OMMetadataManager omMetadataManager,
          long volumeId, long bucketId, long trxnLogIndex,
          List<OmDirectoryInfo> missingParentInfos,
          OmDirectoryInfo dirInfo) {

    for (OmDirectoryInfo subDirInfo : missingParentInfos) {
      addDirectoryTableCacheEntry(omMetadataManager, volumeId, bucketId, trxnLogIndex, subDirInfo);
    }
    if (dirInfo != null) {
      addDirectoryTableCacheEntry(omMetadataManager, volumeId, bucketId, trxnLogIndex, dirInfo);
    }
  }

  private static void addDirectoryTableCacheEntry(OMMetadataManager metadataManager, long volumeId, long bucketId,
      long transactionIndex, OmDirectoryInfo directory) {
    metadataManager.getDirectoryTable().addCacheEntry(metadataManager.getOzonePathKey(volumeId, bucketId, directory),
        directory, transactionIndex);
    if (metadataManager.isNfsObjectIndexEnabled()) {
      metadataManager.getNfsObjectTable().addCacheEntry(
          metadataManager.getNfsObjectKey(volumeId, bucketId, directory.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, directory.getParentObjectID(),
              directory.getName()),
          transactionIndex);
    }
  }

  /** Advance the bucket-root directory verifier in the namespace mutation cache transaction. */
  public static OmBucketInfo updateNfsBucketVerifierCacheEntry(OMMetadataManager metadataManager,
      OmBucketInfo bucketInfo, long transactionIndex) throws IOException {
    if (!metadataManager.isNfsObjectIndexEnabled() || !metadataManager.isNfsObjectIndexReady()) {
      return bucketInfo;
    }
    String bucketKey = metadataManager.getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    OmBucketInfo updated = bucketInfo.toBuilder().setUpdateID(transactionIndex).build();
    metadataManager.getBucketTable().addCacheEntry(bucketKey, updated, transactionIndex);
    return updated;
  }

  /** Advance a directory's direct-child verifier in the namespace mutation cache transaction. */
  public static OmDirectoryInfo updateNfsDirectoryVerifierCacheEntry(OMMetadataManager metadataManager,
      long volumeId, long bucketId, long directoryObjectId, long transactionIndex) throws IOException {
    if (!metadataManager.isNfsObjectIndexEnabled() || !metadataManager.isNfsObjectIndexReady()) {
      return null;
    }
    OmNfsObjectInfo pointer = metadataManager.getNfsObjectTable().get(
        metadataManager.getNfsObjectKey(volumeId, bucketId, directoryObjectId));
    if (pointer == null || pointer.getKind() != PersistedNfsObjectInfo.Kind.DIRECTORY) {
      throw new OMException("NFS directory verifier parent is missing", OMException.ResultCodes.INTERNAL_ERROR);
    }
    String directoryKey = metadataManager.getOzonePathKey(volumeId, bucketId,
        pointer.getParentObjectID(), pointer.getName());
    OmDirectoryInfo directory = metadataManager.getDirectoryTable().get(directoryKey);
    if (directory == null || directory.getObjectID() != directoryObjectId) {
      throw new OMException("NFS directory verifier parent is stale", OMException.ResultCodes.INTERNAL_ERROR);
    }
    OmDirectoryInfo updated = directory.toBuilder().setUpdateID(transactionIndex).build();
    metadataManager.getDirectoryTable().addCacheEntry(directoryKey, updated, transactionIndex);
    return updated;
  }

  /** Adds the canonical directory and its identity pointer to the same database batch. */
  public static void addToDirectoryTable(OMMetadataManager metadataManager, BatchOperation batch,
      OmDirectoryInfo directory, long volumeId, long bucketId) throws IOException {
    metadataManager.getDirectoryTable().putWithBatch(batch,
        metadataManager.getOzonePathKey(volumeId, bucketId, directory), directory);
    if (metadataManager.isNfsObjectIndexEnabled()) {
      metadataManager.getNfsObjectTable().putWithBatch(batch,
          metadataManager.getNfsObjectKey(volumeId, bucketId, directory.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, directory.getParentObjectID(),
              directory.getName()));
    }
  }

  /** Tombstones an identity pointer in the canonical mutation's cache transaction. */
  public static void deleteNfsObjectCacheEntry(OMMetadataManager metadataManager, String indexKey,
      long transactionIndex) {
    if (metadataManager.isNfsObjectIndexEnabled()) {
      metadataManager.getNfsObjectTable().addCacheEntry(indexKey, transactionIndex);
    }
  }

  /** Removes an identity pointer in the canonical mutation's database batch. */
  public static void deleteNfsObjectFromDB(OMMetadataManager metadataManager, BatchOperation batch,
      long volumeId, long bucketId, long objectId) throws IOException {
    if (metadataManager.isNfsObjectIndexEnabled()) {
      metadataManager.getNfsObjectTable().deleteWithBatch(batch,
          metadataManager.getNfsObjectKey(volumeId, bucketId, objectId));
    }
  }

  /**
   * Adding Key info to the openFile Table cache.
   *
   * @param omMetadataManager OM Metadata Manager
   * @param dbOpenFileName    open file name key
   * @param omFileInfo        key info
   * @param trxnLogIndex      transaction log index
   */
  public static void addOpenFileTableCacheEntry(
          OMMetadataManager omMetadataManager, String dbOpenFileName,
          @Nullable OmKeyInfo omFileInfo, String keyName, long trxnLogIndex) {

    final Table<String, OmKeyInfo> table = omMetadataManager.getOpenKeyTable(
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    if (omFileInfo != null) {
      // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
      // keyName field stores full path, which is '/a/b/c/d/e/file1'.
      // This is required as in some cases like hsync, Keys inside openKeyTable is used for auto commit after expiry.
      // (Full key path is required in commit key request)
      omFileInfo.setKeyName(keyName);
      table.addCacheEntry(dbOpenFileName, omFileInfo, trxnLogIndex);
    } else {
      table.addCacheEntry(dbOpenFileName, trxnLogIndex);
    }
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
      OmKeyInfo omFileInfo, String fileName, long trxnLogIndex)
      throws IOException {

    // New key format for the fileTable.
    // For example, the user given key path is '/a/b/c/d/e/file1', then in DB
    // keyName field stores only the leaf node name, which is 'file1'.
    omFileInfo.setKeyName(fileName);

    BucketLayout bucketLayout =
        getBucketLayout(omMetadataManager, omFileInfo.getVolumeName(),
            omFileInfo.getBucketName());

    omMetadataManager.getKeyTable(bucketLayout)
        .addCacheEntry(dbFileKey, omFileInfo, trxnLogIndex);
    if (omMetadataManager.isNfsObjectIndexEnabled()) {
      long volumeId = omMetadataManager.getVolumeId(omFileInfo.getVolumeName());
      long bucketId = omMetadataManager.getBucketId(omFileInfo.getVolumeName(), omFileInfo.getBucketName());
      omMetadataManager.getNfsObjectTable().addCacheEntry(
          omMetadataManager.getNfsObjectKey(volumeId, bucketId, omFileInfo.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, omFileInfo.getParentObjectID(), fileName),
          trxnLogIndex);
    }
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
      BatchOperation batchOp, OmKeyInfo omFileInfo, long openKeySessionID,
      long volumeId, long bucketId) throws IOException {

    String dbOpenFileKey = omMetadataMgr.getOpenFileName(volumeId, bucketId,
            omFileInfo.getParentObjectID(), omFileInfo.getFileName(),
            openKeySessionID);

    omMetadataMgr.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .putWithBatch(batchOp, dbOpenFileKey, omFileInfo);
  }

  /**
   * Adding multipart omKeyInfo to open file table.
   *
   * @param omMetadataMgr OM Metadata Manager
   * @param batchOp       batch of db operations
   * @param omFileInfo    omKeyInfo
   * @param uploadID      uploadID
   * @return multipartFileKey
   * @throws IOException DB failure
   */
  public static String addToOpenFileTableForMultipart(
      OMMetadataManager omMetadataMgr,
      BatchOperation batchOp, OmKeyInfo omFileInfo, String uploadID,
      long volumeId, long bucketId) throws IOException {

    String multipartFileKey = omMetadataMgr.getMultipartKey(volumeId,
            bucketId, omFileInfo.getParentObjectID(),
            omFileInfo.getFileName(), uploadID);

    omMetadataMgr.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .putWithBatch(batchOp, multipartFileKey, omFileInfo);

    return multipartFileKey;
  }

  /**
   * Adding omKeyInfo to file table.
   *
   * @param omMetadataMgr
   * @param batchOp
   * @param omFileInfo
   * @return db file key
   * @throws IOException
   */
  public static String addToFileTable(OMMetadataManager omMetadataMgr,
      BatchOperation batchOp, OmKeyInfo omFileInfo, long volumeId,
      long bucketId) throws IOException {

    String dbFileKey = omMetadataMgr.getOzonePathKey(volumeId, bucketId,
            omFileInfo.getParentObjectID(), omFileInfo.getFileName());

    omMetadataMgr.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .putWithBatch(batchOp, dbFileKey, omFileInfo);
    if (omMetadataMgr.isNfsObjectIndexEnabled()) {
      omMetadataMgr.getNfsObjectTable().putWithBatch(batchOp,
          omMetadataMgr.getNfsObjectKey(volumeId, bucketId, omFileInfo.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, omFileInfo.getParentObjectID(),
              omFileInfo.getFileName()));
    }
    return dbFileKey;
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
      dbOmKeyInfo =
          omMetadataMgr.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
              .get(dbOpenFileKey);
    } else {
      dbOmKeyInfo =
          omMetadataMgr.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
              .get(dbOpenFileKey);
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
   * Gets OmKeyInfo if exists and validate bucket and volume.
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
      OMMetadataManager omMetadataMgr,
      String volumeName, String bucketName, String keyName,
      long scmBlockSize, ReplicationConfig defaultReplication
  ) throws IOException {
    return getOMKeyInfoIfExists(omMetadataMgr, volumeName, bucketName, keyName,
        scmBlockSize, defaultReplication, true);
  }


  /**
   * Gets OmKeyInfo if exists for the given key name in the DB.
   *
   * @param omMetadataMgr metadata manager
   * @param volumeName    volume name
   * @param bucketName    bucket name
   * @param keyName       key name
   * @param scmBlockSize  scm block size
   * @param validateBucketAndVolume true if the volume/bucket needs validating.
   * @return OzoneFileStatus
   * @throws IOException DB failure
   */
  @Nullable
  public static OzoneFileStatus getOMKeyInfoIfExists(
      OMMetadataManager omMetadataMgr,
      String volumeName, String bucketName, String keyName,
      long scmBlockSize, ReplicationConfig defaultReplication,
      boolean validateBucketAndVolume
  ) throws IOException {

    if (validateBucketAndVolume) {
      OMFileRequest.validateBucket(omMetadataMgr, volumeName, bucketName);
    }

    Iterator<String> elements = getKeyPathComponents(keyName).iterator();
    final long volumeId = omMetadataMgr.getVolumeId(volumeName);
    String bucketKey = omMetadataMgr.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataMgr.getBucketTable().get(bucketKey);

    long lastKnownParentId = omBucketInfo.getObjectID();
    OmDirectoryInfo omDirInfo = null;
    while (elements.hasNext()) {
      String fileName = elements.next();

      // For example, /vol1/buck1/a/b/c/d/e/file1.txt
      // 1. Do lookup path component on directoryTable starting from bucket
      // 'buck1' to the leaf node component, which is 'file1.txt'.
      // 2. If there is no dir exists for the leaf node component 'file1.txt'
      // then do look it on fileTable.
      String dbNodeName = omMetadataMgr.getOzonePathKey(
              volumeId, omBucketInfo.getObjectID(),
              lastKnownParentId, fileName);

      if (elements.hasNext()) {
        // Intermediate path component: only its objectID is needed to resolve the next one.
        final Long dirObjectId = getDirObjectId(omMetadataMgr, dbNodeName);
        if (dirObjectId == null) {
          // Missing intermediate directory and just return null;
          // key not found in DB
          return null;
        }
        lastKnownParentId = dirObjectId;
        continue;
      }

      // Last path component: the whole directory is returned to the caller, so read it in full.
      omDirInfo = omMetadataMgr.getDirectoryTable().get(dbNodeName);
      if (omDirInfo == null) {
        if (keyName.endsWith(PATH_SEPARATOR_STR)) {
          // If the requested keyName contains "/" at the end then we need to
          // just check the directory table.
          return null;
        }
        // reached last path component. Check file exists for the given path.
        OmKeyInfo omKeyInfo = OMFileRequest.getOmKeyInfoFromFileTable(false,
                omMetadataMgr, dbNodeName, keyName);
        if (omKeyInfo != null) {
          return new OzoneFileStatus(omKeyInfo, scmBlockSize, false);
        }
      }
    }

    if (omDirInfo != null) {
      OmKeyInfo omKeyInfo = getOmKeyInfo(volumeName, bucketName, omDirInfo,
              keyName);
      ReplicationConfig replicationConfig =
          Optional.ofNullable(omBucketInfo.getDefaultReplicationConfig())
              .map(DefaultReplicationConfig::getReplicationConfig)
              .orElse(defaultReplication);
      omKeyInfo.setReplicationConfig(replicationConfig);
      return new OzoneFileStatus(omKeyInfo, scmBlockSize, true);
    }

    // key not found in DB
    return null;
  }

  public static OmKeyInfo getKeyInfoWithFullPath(OmKeyInfo parentInfo, OmDirectoryInfo directoryInfo) {
    String dirName = OMFileRequest.getAbsolutePath(parentInfo.getKeyName(),
        directoryInfo.getName());
    return OMFileRequest.getOmKeyInfo(
        parentInfo.getVolumeName(), parentInfo.getBucketName(), directoryInfo,
        dirName);
  }

  public static OmKeyInfo getKeyInfoWithFullPath(OmKeyInfo parentInfo, OmKeyInfo omKeyInfo) {
    String fullKeyPath = OMFileRequest.getAbsolutePath(
        parentInfo.getKeyName(), omKeyInfo.getKeyName());
    omKeyInfo.setKeyName(fullKeyPath);
    return omKeyInfo;
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
  @Nonnull
  public static OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
      OmDirectoryInfo dirInfo, String keyName) {

    return dirInfo.toKeyInfoBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
  }

  /**
   * Returns absolute path.
   *
   * @param prefixName prefix path
   * @param fileName   file name
   * @return absolute path
   */
  @Nonnull
  public static String getAbsolutePath(String prefixName, String fileName) {
    if (Strings.isNullOrEmpty(prefixName)) {
      return fileName;
    }
    prefixName = OzoneFSUtils.addTrailingSlashIfNeeded(prefixName);
    return prefixName.concat(fileName);
  }

  /**
   * Build DirectoryInfo from OmKeyInfo.
   *
   * @param keyInfo omKeyInfo
   * @return omDirectoryInfo object
   */
  public static OmDirectoryInfo getDirectoryInfo(OmKeyInfo keyInfo) {
    return keyInfo.toDirectoryInfoBuilder().build();
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
    List<String> fromComponents = getKeyPathComponents(fromKeyName);
    List<String> toComponents = getKeyPathComponents(toKeyName);
    for (int parentDepth = toComponents.size() - 1; parentDepth > 0; parentDepth--) {
      if (fromComponents.equals(toComponents.subList(0, parentDepth))) {
        throw new OMException("Cannot rename a directory to its own " +
                "subdirectory", OMException.ResultCodes.KEY_RENAME_ERROR);
        // TODO: Existing rename throws java.lang.IllegalArgumentException.
        //       Should we throw same exception ?
      }
    }
  }

  /**
   * Verify parent exists for the destination path and return destination
   * path parent.
   * <p>
   * Check whether dst parent dir exists or not. If the parent exists, then the
   * source can be renamed to dst path.
   *
   * @param volumeName volume name
   * @param bucketName bucket name
   * @param toKeyName destination path
   * @param ozoneManager
   * @param metaMgr metadata manager
   * @return omDirectoryInfo object of destination path's parent
   * or null if parent is bucket
   * @throws IOException if the destination parent is not a directory.
   */
  public static OmKeyInfo getKeyParentDir(
      String volumeName, String bucketName, String toKeyName,
      OzoneManager ozoneManager, OMMetadataManager metaMgr) throws IOException {
    int totalDirsCount = OzoneFSUtils.getFileCount(toKeyName);
    // skip parent is root '/'
    if (totalDirsCount <= 1) {
      return null;
    }
    String toKeyParentDir = OzoneFSUtils.getParentDir(toKeyName);
    OzoneFileStatus toKeyParentDirStatus = getOMKeyInfoIfExists(
        metaMgr, volumeName, bucketName, toKeyParentDir, 0,
        ozoneManager.getDefaultReplicationConfig());
    // check if the immediate parent exists
    if (toKeyParentDirStatus == null) {
      throw new OMException(String.format(
              "Failed to get parent dir %s, %s doesn't exist",
              toKeyName, toKeyParentDir),
              OMException.ResultCodes.KEY_RENAME_ERROR);
    } else if (toKeyParentDirStatus.isFile()) {
      throw new OMException(String.format(
              "Failed to get parent dir %s, %s is a file", toKeyName,
              toKeyParentDir), OMException.ResultCodes.KEY_RENAME_ERROR);
    }
    return toKeyParentDirStatus.getKeyInfo();
  }

  /**
   * Check if there are any sub path exist for the given user key path.
   *
   * @param omKeyInfo       om key path
   * @param metaMgr   OMMetadataManager
   * @return true if there are any sub path, false otherwise
   * @throws IOException DB exception
   */
  public static boolean hasChildren(OmKeyInfo omKeyInfo,
                                    OMMetadataManager metaMgr)
      throws IOException {
    return checkSubDirectoryExists(omKeyInfo, metaMgr) ||
        checkSubFileExists(omKeyInfo, metaMgr);
  }

  private static boolean checkSubDirectoryExists(OmKeyInfo omKeyInfo,
      OMMetadataManager metaMgr) throws IOException {
    // Check all dirTable cache for any sub paths.
    Table dirTable = metaMgr.getDirectoryTable();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>>>
            cacheIter = dirTable.cacheIterator();

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmDirectoryInfo>> entry =
              cacheIter.next();
      OmDirectoryInfo cacheOmDirInfo = entry.getValue().getCacheValue();
      if (cacheOmDirInfo == null) {
        continue;
      }
      if (isImmediateChild(cacheOmDirInfo.getParentObjectID(),
              omKeyInfo.getObjectID())) {
        return true; // found a sub path directory
      }
    }

    final long volumeId = metaMgr.getVolumeId(omKeyInfo.getVolumeName());
    final long bucketId = metaMgr.getBucketId(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName());
    // Check dirTable entries for any sub paths.
    String seekDirInDB = metaMgr.getOzonePathKey(volumeId, bucketId,
            omKeyInfo.getObjectID(), "");
    try (TableIterator<String, ? extends
        Table.KeyValue<String, OmDirectoryInfo>>
            iterator = dirTable.iterator(seekDirInDB)) {

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> entry = iterator.next();
        String dbKey = entry.getKey();
        OmDirectoryInfo dirInfo = entry.getValue();
        boolean isChild = isImmediateChild(dirInfo.getParentObjectID(),
            omKeyInfo.getObjectID());

        if (!isChild) {
          return false;
        }

        // If child found in DB, check if it's marked as deleted in cache
        CacheValue<OmDirectoryInfo> cacheValue = dirTable.getCacheValue(new CacheKey<>(dbKey));
        if (cacheValue != null && cacheValue.getCacheValue() == null) {
          // Entry is in DB but marked for deletion in cache, ignore it and check next entry
          continue;
        }

        return true;
      }

    }
    return false; // no sub paths found
  }

  private static boolean checkSubFileExists(OmKeyInfo omKeyInfo,
                                            OMMetadataManager metaMgr)
      throws IOException {
    BucketLayout bucketLayout =
        getBucketLayout(metaMgr, omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName());
    // Check all fileTable cache for any sub paths.
    Table fileTable = metaMgr.getKeyTable(bucketLayout);
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>>
            cacheIter = fileTable.cacheIterator();

    while (cacheIter.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry =
              cacheIter.next();
      OmKeyInfo cacheOmFileInfo = entry.getValue().getCacheValue();
      if (cacheOmFileInfo == null) {
        continue;
      }
      if (isImmediateChild(cacheOmFileInfo.getParentObjectID(),
              omKeyInfo.getObjectID())) {
        return true; // found a sub path file
      }
    }

    final long volumeId = metaMgr.getVolumeId(omKeyInfo.getVolumeName());
    final long bucketId = metaMgr.getBucketId(omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName());
    // Check fileTable entries for any sub paths.
    String seekFileInDB = metaMgr.getOzonePathKey(volumeId, bucketId,
            omKeyInfo.getObjectID(), "");
    try (TableIterator<String, Table.KeyValue<String, OmKeyInfo>>
            iterator = fileTable.iterator(seekFileInDB)) {

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> entry = iterator.next();
        String dbKey = entry.getKey();
        OmKeyInfo fileInfo = entry.getValue();
        boolean isChild = isImmediateChild(fileInfo.getParentObjectID(),
            omKeyInfo.getObjectID());

        if (!isChild) {
          return false;
        }

        // If child found in DB, check if it's marked as deleted in cache
        CacheValue<OmKeyInfo> cacheValue = fileTable.getCacheValue(new CacheKey<>(dbKey));
        if (cacheValue != null && cacheValue.getCacheValue() == null) {
          // Entry is in DB but marked for deletion in cache, ignore it and check next entry
          continue;
        }

        return true; // found a sub path file
      }
    }
    return false; // no sub paths found
  }

  public static boolean isImmediateChild(long parentId, long ancestorId) {
    return parentId == ancestorId;
  }

  /**
   * Get parent id for the user given path.
   *
   * @param volumeId       volume id
   * @param bucketId       bucket id
   * @param keyName        user given key name
   * @param omMetadataManager   om metadata manager
   * @return lastKnownParentID
   * @throws IOException DB failure or parent not exists in DirectoryTable
   */
  public static long getParentID(long volumeId, long bucketId,
                                 String keyName,
                                 OMMetadataManager omMetadataManager)
      throws IOException {
    return getParentID(volumeId, bucketId, keyName,
            omMetadataManager, null);
  }

  /**
   * Get parent id for the user given path.
   *
   * @param bucketId       bucket id
   * @param keyName        user given key name
   * @param omMetadataManager   om metadata manager
   * @return lastKnownParentID
   * @throws IOException DB failure or parent not exists in DirectoryTable
   */
  public static long getParentID(long volumeId, long bucketId, String keyName,
      OMMetadataManager omMetadataManager, String errMsg)
      throws IOException {

    long lastKnownParentId = bucketId;
    Iterator<String> pathComponents = getKeyPathComponents(keyName).iterator();

    // If no sub-dirs then bucketID is the root/parent.
    if (!pathComponents.hasNext()) {
      return bucketId;
    }
    if (StringUtils.isBlank(errMsg)) {
      errMsg = "Failed to find parent directory of " + keyName;
    }
    while (pathComponents.hasNext()) {
      String nodeName = pathComponents.next();
      boolean reachedLastPathComponent = !pathComponents.hasNext();
      String dbNodeName =
              omMetadataManager.getOzonePathKey(volumeId, bucketId,
                      lastKnownParentId, nodeName);


      final Long dirObjectId = getDirObjectId(omMetadataManager, dbNodeName);
      if (dirObjectId != null) {
        if (reachedLastPathComponent) {
          throw new OMException("Can not create file: " + keyName +
                  " as there is already directory in the given path",
                  NOT_A_FILE);
        }
        lastKnownParentId = dirObjectId;
      } else {
        // One of the sub-dir doesn't exists in DB. Immediate parent should
        // exists for committing the key, otherwise will fail the operation.
        if (!reachedLastPathComponent) {
          throw new OMException(errMsg,
              OMException.ResultCodes.DIRECTORY_NOT_FOUND);
        }
        break;
      }
    }

    return lastKnownParentId;
  }

  /**
   * Get parent ID for the user given keyName.
   *
   * @param omMetadataManager
   * @param volumeName        - volume name.
   * @param bucketName        - bucket name.
   * @param keyName           - key name.
   * @return {@code long}
   * @throws IOException
   */
  public static long getParentId(OMMetadataManager omMetadataManager,
                                 String volumeName, String bucketName,
                                 String keyName)
      throws IOException {


    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    return OMFileRequest.getParentID(volumeId, bucketId,
            keyName, omMetadataManager);
  }

  /**
   * Validates volume and bucket existence.
   *
   * @param metadataManager
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  public static void validateBucket(OMMetadataManager metadataManager,
      String volumeName, String bucketName)
      throws IOException {

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    // Check if bucket exists
    if (metadataManager.getBucketTable().get(bucketKey) == null) {
      String volumeKey = metadataManager.getVolumeKey(volumeName);
      // If the volume also does not exist, we should throw volume not found
      // exception
      if (metadataManager.getVolumeTable().get(volumeKey) == null) {
        LOG.error("volume not found: {}", volumeName);
        throw new OMException("Volume not found",
            VOLUME_NOT_FOUND);
      }

      // if the volume exists but bucket does not exist, throw bucket not found
      // exception
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          BUCKET_NOT_FOUND);
    }
  }

}
