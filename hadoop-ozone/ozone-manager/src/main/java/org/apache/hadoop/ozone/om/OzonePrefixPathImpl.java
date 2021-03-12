/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.security.acl.OzonePrefixPath;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class OzonePrefixPathImpl implements OzonePrefixPath {

  private String volumeName;
  private String bucketName;
  private KeyManager keyManager;
  private int batchSize = 1000; // TODO: can be configurable.
  private OzoneFileStatus pathStatus;

  public OzonePrefixPathImpl(String volumeName, String bucketName,
      String keyPrefix, KeyManager keyManagerImpl) throws IOException {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyManager = keyManagerImpl;

    OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyPrefix)
        .setRefreshPipeline(false)
        .build();
    pathStatus = keyManager.getFileStatus(omKeyArgs);
  }

  @Override
  public OzoneFileStatus getOzonePrefixPath() {
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
        try {
          currentIterator =
              getNextListOfKeys(currentValue.getTrimmedName()).iterator();
        } catch (IOException e) {
          throw new RuntimeException(e);
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
          .setRefreshPipeline(false)
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
}
