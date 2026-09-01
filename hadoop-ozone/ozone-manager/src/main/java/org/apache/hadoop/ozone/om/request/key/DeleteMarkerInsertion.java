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

package org.apache.hadoop.ozone.om.request.key;

import java.util.Map;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * What inserting a delete marker changed, for the response to write out.
 *
 * <p>Produced by {@link OMKeyRequest#insertDeleteMarker}, which has already
 * applied all of it to the table cache and to the bucket's quota. A response
 * only has to put the same records into its WriteBatch.
 */
public final class DeleteMarkerInsertion {

  private final OmKeyInfo deleteMarker;
  private final String objectKey;
  private final String demotedVersionKey;
  private final OmKeyInfo demotedVersion;
  private final String replacedNullVersionKey;
  private final Map<String, RepeatedOmKeyInfo> keysToDelete;

  @SuppressWarnings("checkstyle:ParameterNumber")
  DeleteMarkerInsertion(OmKeyInfo deleteMarker, String objectKey,
      String demotedVersionKey, OmKeyInfo demotedVersion,
      String replacedNullVersionKey,
      Map<String, RepeatedOmKeyInfo> keysToDelete) {
    this.deleteMarker = deleteMarker;
    this.objectKey = objectKey;
    this.demotedVersionKey = demotedVersionKey;
    this.demotedVersion = demotedVersion;
    this.replacedNullVersionKey = replacedNullVersionKey;
    this.keysToDelete = keysToDelete;
  }

  /**
   * versionedKeyTable dbKey of the null version the marker replaced while
   * versioning was suspended, or null when nothing was replaced.
   */
  public String getReplacedNullVersionKey() {
    return replacedNullVersionKey;
  }

  /** Blocks of the replaced null version, queued for reclamation. */
  public Map<String, RepeatedOmKeyInfo> getKeysToDelete() {
    return keysToDelete;
  }

  /** The marker that becomes the key's current version. */
  public OmKeyInfo getDeleteMarker() {
    return deleteMarker;
  }

  /** keyTable dbKey the marker takes. */
  public String getObjectKey() {
    return objectKey;
  }

  /**
   * versionedKeyTable dbKey the superseded version moves to, or null when the
   * key had no current version.
   */
  public String getDemotedVersionKey() {
    return demotedVersionKey;
  }

  public OmKeyInfo getDemotedVersion() {
    return demotedVersion;
  }
}
