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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import com.google.common.base.Preconditions;

/**
 * A DB key of openKeyTable or openFileTable, parsed into the DB key of the key and the suffix after it.
 * <p>
 * The DB keys of the two tables are different:
 * <ul>
 *   <li>openKeyTable: /{volumeName}/{bucketName}/{keyName}/{suffix}</li>
 *   <li>openFileTable: /{volumeId}/{bucketId}/{parentId}/{fileName}/{suffix}</li>
 * </ul>
 * The suffix is the client ID, or the upload ID for the open key of a multipart upload.
 * Upload ID format: uploadId = UUID + "-" + UniqueId#next
 * <p>
 * The open key of a part being written ends with a client ID, so {@link #isUpload()} is false for it.
 */
public final class OpenKeyDBKey {
  private final String keyTableDBKey;
  private final String suffix;
  private final boolean isUpload;

  /**
   * @param dbKey a DB key of openKeyTable or openFileTable
   * @throws IllegalArgumentException if dbKey has no "/"
   */
  public OpenKeyDBKey(String dbKey) {
    final int lastPrefix = dbKey.lastIndexOf(OM_KEY_PREFIX);
    Preconditions.checkArgument(lastPrefix >= 0, "Not an open key DB key: %s", dbKey);
    this.keyTableDBKey = dbKey.substring(0, lastPrefix);
    this.suffix = dbKey.substring(lastPrefix + 1);
    // Similar to the logic of UUID#fromString, but use 6 since there is another "-" between the UUID and the UniqueId
    this.isUpload = !suffix.isEmpty() && suffix.split("-").length == 6;
  }

  /** @return the DB key without the suffix, i.e. the DB key of keyTable or fileTable. */
  public String getKeyTableDBKey() {
    return keyTableDBKey;
  }

  /** @return true if the suffix is an upload ID. */
  public boolean isUpload() {
    return isUpload;
  }

  /** @return the upload ID, or null if the suffix is a client ID. */
  public String getUploadId() {
    return isUpload ? suffix : null;
  }

  /**
   * @return the client ID.
   * @throws IllegalStateException if the suffix is an upload ID
   */
  public long getClientId() {
    Preconditions.checkState(!isUpload, "No client ID in the open key of upload %s", suffix);
    return Long.parseLong(suffix);
  }
}
