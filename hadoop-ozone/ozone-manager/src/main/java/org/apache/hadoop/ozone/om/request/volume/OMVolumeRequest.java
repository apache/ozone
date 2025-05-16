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

package org.apache.hadoop.ozone.om.request.volume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;

/**
 * Defines common methods required for volume requests.
 */
public abstract class OMVolumeRequest extends OMClientRequest {

  public OMVolumeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * Delete volume from user volume list. This method should be called after
   * acquiring user lock.
   * @param volumeList - current volume list owned by user.
   * @param volume - volume which needs to deleted from the volume list.
   * @param owner - Name of the Owner.
   * @param txID - The transaction ID that is updating this value.
   * @return UserVolumeInfo - updated UserVolumeInfo.
   * @throws IOException
   */
  protected PersistedUserVolumeInfo delVolumeFromOwnerList(
      PersistedUserVolumeInfo volumeList, String volume,
      String owner, long txID) throws IOException {

    List<String> prevVolList = new ArrayList<>();

    if (volumeList != null) {
      prevVolList.addAll(volumeList.getVolumeNamesList());
    } else {
      // No Volumes for this user
      throw new OMException("User not found: " + owner,
          OMException.ResultCodes.USER_NOT_FOUND);
    }

    // Remove the volume from the list
    prevVolList.remove(volume);
    PersistedUserVolumeInfo newVolList = PersistedUserVolumeInfo.newBuilder()
        .addAllVolumeNames(prevVolList)
            .setObjectID(volumeList.getObjectID())
            .setUpdateID(txID)
         .build();
    return newVolList;
  }

  /**
   * Add volume to user volume list. This method should be called after
   * acquiring user lock.
   * @param volumeList - current volume list owned by user.
   * @param volume - volume which needs to be added to this list.
   * @param owner
   * @param maxUserVolumeCount
   * @return VolumeList - which is updated volume list.
   * @throws OMException - if user has volumes greater than
   * maxUserVolumeCount, an exception is thrown.
   */
  protected static PersistedUserVolumeInfo addVolumeToOwnerList(
      PersistedUserVolumeInfo volumeList, String volume, String owner,
      long maxUserVolumeCount, long txID) throws IOException {

    // Check the volume count
    if (volumeList != null &&
        volumeList.getVolumeNamesList().size() >= maxUserVolumeCount) {
      throw new OMException("Too many volumes for user:" + owner,
          OMException.ResultCodes.USER_TOO_MANY_VOLUMES);
    }

    Set<String> volumeSet = new HashSet<>();
    long objectID = txID;
    if (volumeList != null) {
      volumeSet.addAll(volumeList.getVolumeNamesList());
      objectID = volumeList.getObjectID();
    }

    volumeSet.add(volume);
    return PersistedUserVolumeInfo.newBuilder()
        .setObjectID(objectID)
        .setUpdateID(txID)
        .addAllVolumeNames(volumeSet).build();
  }

  /**
   * Create Ozone Volume. This method should be called after acquiring user
   * and volume Lock.
   * @param omMetadataManager
   * @param omVolumeArgs
   * @param volumeList
   * @param dbVolumeKey
   * @param dbUserKey
   * @param transactionLogIndex
   */
  protected static void createVolume(
      final OMMetadataManager omMetadataManager, OmVolumeArgs omVolumeArgs,
      PersistedUserVolumeInfo volumeList, String dbVolumeKey,
      String dbUserKey, long transactionLogIndex) {
    // Update cache: Update user and volume cache.
    omMetadataManager.getUserTable().addCacheEntry(new CacheKey<>(dbUserKey),
        CacheValue.get(transactionLogIndex, volumeList));

    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(dbVolumeKey),
        CacheValue.get(transactionLogIndex, omVolumeArgs));
  }

  /**
   * Return volume info for the specified volume. This method should be
   * called after acquiring volume lock.
   * @param omMetadataManager
   * @param volume
   * @return OmVolumeArgs
   * @throws IOException
   */
  protected OmVolumeArgs getVolumeInfo(OMMetadataManager omMetadataManager,
      String volume) throws IOException {

    String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs =
        omMetadataManager.getVolumeTable().get(dbVolumeKey);
    if (volumeArgs == null) {
      throw new OMException("Volume " + volume + " is not found",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }
    return volumeArgs;
  }
}
