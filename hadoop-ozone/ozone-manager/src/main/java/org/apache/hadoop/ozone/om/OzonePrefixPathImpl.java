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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of OzonePrefixPath interface.
 */
public class OzonePrefixPathImpl implements OzonePrefixPath {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzonePrefixPathImpl.class);
  private String volumeName;
  private String bucketName;
  private KeyManager keyManager;
  // TODO: based on need can make batchSize configurable.
  private int batchSize = 1000;
  private OzoneFileStatus pathStatus;
  private boolean checkRecursiveAccess = false;

  public OzonePrefixPathImpl(String volumeName, String bucketName,
      String keyPrefix, KeyManager keyManagerImpl) throws IOException {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyManager = keyManagerImpl;

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyPrefix)
        .setHeadOp(true)
        .build();
    try {
      pathStatus = keyManager.getFileStatus(omKeyArgs);
    } catch (OMException ome) {
      // In existing code non-FSO code, ozone client delete and rename expects
      // KNF error code. So converting FNF to KEY_NOT_FOUND error code.
      if (ome.getResult() == OMException.ResultCodes.FILE_NOT_FOUND) {
        throw new OMException(ome.getMessage(), KEY_NOT_FOUND);
      }
      throw ome;
    }

    // Check if this key is a directory.
    // NOTE: checkRecursiveAccess is always false for a file keyPrefix.
    if (pathStatus != null && pathStatus.isDirectory()) {
      // set recursive access check to true if this directory contains
      // sub-directories or sub-files.
      checkRecursiveAccess = OMFileRequest.hasChildren(
          pathStatus.getKeyInfo(), keyManager.getMetadataManager());
    }
  }

  @Override
  public OzoneFileStatus getOzoneFileStatus() {
    return pathStatus;
  }

  @Override
  public Iterator<? extends OzoneFileStatus> getChildren(String keyPrefix)
      throws IOException {

    return new PathIterator(keyPrefix);
  }

  class PathIterator implements Iterator<OzoneFileStatus> {
    private Iterator<OzoneFileStatus> currentIterator;
    private String keyPrefix;
    private OzoneFileStatus currentValue;

    /**
     * Creates an Iterator to iterate over all sub paths of the given keyPrefix.
     *
     * @param keyPrefix
     */
    PathIterator(String keyPrefix) throws IOException {
      this.keyPrefix = keyPrefix;
      this.currentValue = null;
      List<OzoneFileStatus> statuses = getNextListOfKeys("");
      if (statuses.size() == 1) {
        OzoneFileStatus keyStatus = statuses.get(0);
        if (keyStatus.isFile() && StringUtils.equals(keyPrefix,
            keyStatus.getTrimmedName())) {
          throw new OMException("Invalid keyPrefix: " + keyPrefix +
              ", file type is not allowed, expected directory type.",
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
      }
      this.currentIterator = statuses.iterator();
    }

    @Override
    public boolean hasNext() {
      if (!currentIterator.hasNext() && currentValue != null) {
        String keyName = "";
        try {
          keyName = currentValue.getTrimmedName();
          currentIterator =
              getNextListOfKeys(keyName).iterator();
        } catch (IOException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Exception while listing keys, keyName:" + keyName, e);
          }
          return false;
        }
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneFileStatus next() {
      if (hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Gets the next set of key list using keyManager OM interface.
     *
     * @param prevKey
     * @return {@code List<OzoneFileStatus>}
     */
    List<OzoneFileStatus> getNextListOfKeys(String prevKey) throws
        IOException {

      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setKeyName(keyPrefix)
          .setHeadOp(true)
          .build();

      List<OzoneFileStatus> statuses = keyManager.listStatus(omKeyArgs, false,
          prevKey, batchSize);

      // ListStatuses with non-null startKey will add startKey as first element
      // in the resultList. Remove startKey element as it is duplicated one.
      if (!statuses.isEmpty() && StringUtils.equals(prevKey,
          statuses.get(0).getTrimmedName())) {
        statuses.remove(0);
      }
      return statuses;
    }
  }

  /**
   * @return true if no sub-directories or sub-files exist, false otherwise
   */
  public boolean isCheckRecursiveAccess() {
    return checkRecursiveAccess;
  }
}
