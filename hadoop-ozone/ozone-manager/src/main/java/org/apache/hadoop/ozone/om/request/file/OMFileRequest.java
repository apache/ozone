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
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
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
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      inheritAcls = omMetadataManager.getBucketTable().get(bucketKey).getAcls();
      LOG.trace("Acls inherited from bucket " + bucketName + " are : "
          + inheritAcls);
    }

    LOG.trace("verifyFiles in Path : " + volumeName + "/" + bucketName + "/"
        + keyName + ":" + result);
    // Found no files/ directories in the given path.
    return new OMPathInfo(missing, OMDirectoryResult.NONE, inheritAcls);
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
    private List<String> missingParents;
    private List<OzoneAcl> acls;

    public OMPathInfo(List missingParents, OMDirectoryResult result,
        List<OzoneAcl> aclList) {
      this.missingParents = missingParents;
      directoryResult = result;
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
}
