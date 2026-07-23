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

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.VersionIdGenerator;

/**
 * Assigns the versionId of a version being committed, using the generator
 * configured for this cluster.
 *
 * <p>The generator is cluster-wide and can be changed by reconfiguring the OMs,
 * so ids generated before and after a change are not guaranteed to be distinct.
 * Rather than constrain the change, a commit whose versionId already exists on
 * the key is rejected: the operator or the client deletes the existing version
 * first, and the write can then be retried.
 */
public class VersionIdAllocator {

  private final VersionIdGenerator generator;

  public VersionIdAllocator(ConfigurationSource conf) {
    this(VersionIdGenerator.fromConfiguration(conf));
  }

  public VersionIdAllocator(VersionIdGenerator generator) {
    this.generator = generator;
  }

  public VersionIdGenerator getGenerator() {
    return generator;
  }

  /**
   * Returns the versionId to freeze on the version being committed.
   *
   * <p>The id must be greater than the one on the key's current version: a
   * generator has to hand out increasing ids for a key, and the versionedKeyTable
   * ordering and version promotion depend on it. An id that is not greater is
   * refused rather than written, because it would either overwrite an existing
   * version or sort into the wrong place. In practice this only happens after the
   * cluster's generator is changed, or if the Ratis log index went backwards.
   *
   * @param currentVersion the key's current version, or null if the key has
   *     none. The write path holds it already, so no extra read is needed here.
   * @throws OMException INVALID_REQUEST if the generated id does not exceed the
   *     current version's id, KEY_ALREADY_EXISTS if it is already taken
   */
  public long allocate(OMMetadataManager metadataManager, String volumeName,
      String bucketName, String keyName, long transactionLogIndex,
      OmKeyInfo currentVersion) throws IOException {

    long versionId =
        generator.generateVersionId(transactionLogIndex, currentVersion != null);

    if (currentVersion == null) {
      // No current version means the key has no versions at all, so nothing can
      // be taken and nothing constrains the id.
      return versionId;
    }

    Long currentVersionId = currentVersion.getVersionId();
    if (currentVersionId == null) {
      // A current version written before versioning was enabled carries no id,
      // so there is nothing to order against; fall back to looking the id up.
      if (isTaken(metadataManager, volumeName, bucketName, keyName, versionId)) {
        throw alreadyExists(volumeName, bucketName, keyName, versionId);
      }
      return versionId;
    }

    if (versionId <= currentVersionId) {
      throw new OMException("Version " + versionId + " of key /" + volumeName + "/"
          + bucketName + "/" + keyName + " does not come after the current version "
          + currentVersionId + ". " + generator.getClass().getName()
          + " must generate increasing versionIds for a key; an id that goes backwards can "
          + "happen after the cluster's " + OMConfigKeys.OZONE_OM_VERSIONING_VERSION_ID_GENERATOR
          + " is changed. Delete the key's versions before writing with the new generator.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // Strictly greater than the largest id on the key, so no lookup is needed:
    // every noncurrent version of the key has a smaller id than the current one.
    return versionId;
  }

  private static OMException alreadyExists(String volumeName, String bucketName,
      String keyName, long versionId) {
    return new OMException("Version " + versionId + " of key /" + volumeName + "/"
        + bucketName + "/" + keyName + " already exists. Delete that version before "
        + "writing this one.", OMException.ResultCodes.KEY_ALREADY_EXISTS);
  }

  private boolean isTaken(OMMetadataManager metadataManager, String volumeName,
      String bucketName, String keyName, long versionId) throws IOException {
    return metadataManager.getVersionedKeyTable().isExist(
        metadataManager.getVersionedOzoneKey(volumeName, bucketName, keyName, versionId));
  }

}
