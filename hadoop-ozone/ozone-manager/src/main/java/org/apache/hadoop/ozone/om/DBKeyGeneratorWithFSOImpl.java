/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * DBKeyPathGenerator implementation for File System Optimized buckets.
 */
public class DBKeyGeneratorWithFSOImpl implements DBKeyGenerator {

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneDBKey(OmKeyArgs keyArgs, OzoneManager ozoneManager,
                              String errMsg) throws IOException {
    return getOzoneDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), ozoneManager, errMsg);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneDBKey(OzoneManagerProtocolProtos.KeyArgs keyArgs,
                              OzoneManager ozoneManager, String errMsg)
      throws IOException {
    return getOzoneDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), ozoneManager, errMsg);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneOpenDBKey(OmKeyArgs keyArgs,
                                  OzoneManager ozoneManager, long clientId,
                                  String errMsg)
      throws IOException {
    return getOzoneOpenDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), clientId, ozoneManager, errMsg);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getOzoneOpenDBKey(OzoneManagerProtocolProtos.KeyArgs keyArgs,
                                  OzoneManager ozoneManager, long clientId,
                                  String errMsg) throws IOException {
    return getOzoneOpenDBKey(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getKeyName(), clientId, ozoneManager, errMsg);
  }

  /**
   * Generates the DB key for querying the fileTable.
   * Key is of the format parentID/keyName
   *
   * @param volumeName   volume name
   * @param bucketName   bucket name
   * @param keyName      key name
   * @param ozoneManager ozone manager
   * @param errMsg       error message
   * @return - DB key
   * @throws IOException IOException
   */
  private String getOzoneDBKey(String volumeName, String bucketName,
                               String keyName,
                               OzoneManager ozoneManager, String errMsg)
      throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // Get bucket info.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    CacheValue<OmBucketInfo> value = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));

    OmBucketInfo omBucketInfo = value != null ? value.getCacheValue() :
        omMetadataManager.getBucketTable().get(bucketKey);

    // In case bucket or volume is not present, throw exception.
    if (omBucketInfo == null) {
      // Make sure we throw the correct Exception.
      if (omMetadataManager.getVolumeTable()
          .get(omMetadataManager.getVolumeKey(volumeName)) == null) {
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }
      throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

    // Generate all the components needed to build the ozoneKey.
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    long bucketId = omBucketInfo.getObjectID();
    long parentID = OMFileRequest.getParentID(bucketId, pathComponents,
        keyName, omMetadataManager, errMsg);
    String fileName = OzoneFSUtils.getFileName(keyName);

    // Generate the DB key.
    return omMetadataManager.getOzonePathKey(parentID, fileName);
  }

  /**
   * Generates the DB key for querying the openFileTable.
   * Key is of the format <parent ID>/<key name>/<client ID>
   *
   * @param volumeName   volume name
   * @param bucketName   bucket name
   * @param keyName      key name
   * @param clientId     request client ID
   * @param ozoneManager ozone manager instance
   * @param errMsg       error message
   * @return - DB key
   * @throws IOException IOException
   */
  private String getOzoneOpenDBKey(String volumeName, String bucketName,
                                   String keyName, long clientId,
                                   OzoneManager ozoneManager, String errMsg)
      throws IOException {
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // Get bucket info.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    CacheValue<OmBucketInfo> value = omMetadataManager.getBucketTable()
        .getCacheValue(new CacheKey<>(bucketKey));

    OmBucketInfo omBucketInfo = value != null ? value.getCacheValue() :
        omMetadataManager.getBucketTable().get(bucketKey);

    // In case bucket or volume is not present, throw exception.
    if (omBucketInfo == null) {
      // Make sure we throw the correct Exception.
      if (omMetadataManager.getVolumeTable()
          .get(omMetadataManager.getVolumeKey(volumeName)) == null) {
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }
      throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
    }

    // Generate all the components needed to build the ozoneKey.
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    long bucketId = omBucketInfo.getObjectID();
    long parentID = OMFileRequest.getParentID(bucketId, pathComponents,
        keyName, omMetadataManager, errMsg);
    String fileName = OzoneFSUtils.getFileName(keyName);

    // Generate the key.
    String openKey =
        parentID + OM_KEY_PREFIX + fileName + OM_KEY_PREFIX + clientId;
    return openKey;
  }
}
