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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * BucketLayout enum
 * We have 3 types of bucket layouts - FSO, OBJECT_STORE, and LEGACY
 * LEGACY is used to represent the old buckets which are already
 * present in DB while user can create new buckets as FSO or OBJECT_STORE.
 */
public enum BucketLayout {
  FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY;
  public static final BucketLayout DEFAULT = LEGACY;
  public static BucketLayout fromProto(
      OzoneManagerProtocolProtos.BucketLayoutProto bucketLayout) {
    if (bucketLayout == null) {
      return BucketLayout.LEGACY;
    }
    switch (bucketLayout) {
    case FILE_SYSTEM_OPTIMIZED:
      return BucketLayout.FILE_SYSTEM_OPTIMIZED;
    case LEGACY:
      return BucketLayout.LEGACY;
    case OBJECT_STORE:
      return BucketLayout.OBJECT_STORE;
    default:
      return DEFAULT;
    }
  }

  public OzoneManagerProtocolProtos.BucketLayoutProto toProto() {
    switch (this) {
    case FILE_SYSTEM_OPTIMIZED:
      return OzoneManagerProtocolProtos.BucketLayoutProto.FILE_SYSTEM_OPTIMIZED;
    case OBJECT_STORE:
      return OzoneManagerProtocolProtos.BucketLayoutProto.OBJECT_STORE;
    case LEGACY:
      return OzoneManagerProtocolProtos.BucketLayoutProto.LEGACY;
    default:
      throw new IllegalArgumentException(
          "Error: BucketLayout not found, type=" + this);
    }
  }

  public boolean isFileSystemOptimized() {
    return this.equals(FILE_SYSTEM_OPTIMIZED);
  }

  public boolean isLegacy() {
    return this.equals(LEGACY);
  }

  public boolean isObjectStore(boolean enableFileSystemPaths) {
    if (this.equals(OBJECT_STORE)) {
      return true;
    } else {
      // If bucket layout is Legacy and FileSystemPaths
      // are disabled, then the bucket operates as OBS.
      if (this.equals(LEGACY) && !enableFileSystemPaths) {
        return true;
      }
      return false;
    }
  }

  public boolean shouldNormalizePaths(boolean enableFileSystemPaths) {
    switch (this) {
    case OBJECT_STORE:
      return false;
    case FILE_SYSTEM_OPTIMIZED:
      return true;
    case LEGACY:
      return enableFileSystemPaths;
    default:
      throw new IllegalArgumentException("Invalid Bucket Layout:" + this);
    }
  }

  public static BucketLayout fromString(String value) {
    // This will never be null in production but can be null in mocked
    // unit test cases.
    // Added safer `isBlank` check for unit test cases.
    return StringUtils.isBlank(value) ? LEGACY : BucketLayout.valueOf(value);
  }

  /**
   * Helper method for upgrade scenarios. Throws an exception if a bucket layout
   * is not supported on an older client.
   *
   * @throws OMException if bucket layout is not supported on older clients.
   */
  public void validateSupportedOperation() throws OMException {
    // Older clients do not support any bucket layout other than LEGACY.
    if (!isLegacy()) {
      throw new OMException("Client is attempting to modify a bucket which" +
          " uses non-LEGACY bucket layout features. Please upgrade the client" +
          " to a compatible version before performing this operation.",
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }
}
