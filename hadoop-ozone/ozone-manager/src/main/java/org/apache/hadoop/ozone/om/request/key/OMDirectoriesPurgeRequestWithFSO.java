/*
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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMDirectoriesPurgeResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

import java.util.List;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles purging of keys from OM DB.
 */
public class OMDirectoriesPurgeRequestWithFSO extends OMKeyRequest {

  public OMDirectoriesPurgeRequestWithFSO(OMRequest omRequest) {
    super(omRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex) {
    OzoneManagerProtocolProtos.PurgeDirectoriesRequest purgeDirsRequest =
        getOmRequest().getPurgeDirectoriesRequest();
    String fromSnapshot = purgeDirsRequest.hasSnapshotTableKey() ?
        purgeDirsRequest.getSnapshotTableKey() : null;

    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeRequests =
            purgeDirsRequest.getDeletedPathList();

    SnapshotInfo fromSnapshotInfo = null;
    Set<Pair<String, String>> lockSet = new HashSet<>();
    Map<Pair<String, String>, OmBucketInfo> volBucketInfoMap = new HashMap<>();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    try {
      if (fromSnapshot != null) {
        fromSnapshotInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, fromSnapshot);
      }

      for (OzoneManagerProtocolProtos.PurgePathRequest path : purgeRequests) {
        for (OzoneManagerProtocolProtos.KeyInfo key :
            path.getMarkDeletedSubDirsList()) {
          OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          Pair<String, String> volBucketPair = Pair.of(volumeName, bucketName);
          if (!lockSet.contains(volBucketPair)) {
            omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
                volumeName, bucketName);
            lockSet.add(volBucketPair);
          }

          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              volumeName, bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo
              && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.incrUsedNamespace(-1L);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), keyInfo.getParentObjectID(), keyInfo.getFileName());
            omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(trxnLogIndex));
            volBucketInfoMap.putIfAbsent(volBucketPair, omBucketInfo);
          }
        }

        for (OzoneManagerProtocolProtos.KeyInfo key :
            path.getDeletedSubFilesList()) {
          OmKeyInfo keyInfo = OmKeyInfo.getFromProtobuf(key);
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          Pair<String, String> volBucketPair = Pair.of(volumeName, bucketName);
          if (!lockSet.contains(volBucketPair)) {
            omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
                volumeName, bucketName);
            lockSet.add(volBucketPair);
          }
          OmBucketInfo omBucketInfo = getBucketInfo(omMetadataManager,
              volumeName, bucketName);
          // bucketInfo can be null in case of delete volume or bucket
          // or key does not belong to bucket as bucket is recreated
          if (null != omBucketInfo
              && omBucketInfo.getObjectID() == path.getBucketId()) {
            omBucketInfo.incrUsedBytes(-sumBlockLengths(keyInfo));
            omBucketInfo.incrUsedNamespace(-1L);
            String ozoneDbKey = omMetadataManager.getOzonePathKey(path.getVolumeId(),
                path.getBucketId(), keyInfo.getParentObjectID(), keyInfo.getFileName());
            omMetadataManager.getFileTable().addCacheEntry(new CacheKey<>(ozoneDbKey),
                CacheValue.get(trxnLogIndex));
            volBucketInfoMap.putIfAbsent(volBucketPair, omBucketInfo);
          }
        }
      }
    } catch (IOException ex) {
      // Case of IOException for fromProtobuf will not happen
      // as this is created and send within OM
      // only case of upgrade where compatibility is broken can have
      throw new IllegalStateException(ex);
    } finally {
      lockSet.stream().forEach(e -> omMetadataManager.getLock()
          .releaseWriteLock(BUCKET_LOCK, e.getKey(),
              e.getValue()));
      for (Map.Entry<Pair<String, String>, OmBucketInfo> entry :
          volBucketInfoMap.entrySet()) {
        entry.setValue(entry.getValue().copyObject());
      }
    }

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    OMClientResponse omClientResponse = new OMDirectoriesPurgeResponseWithFSO(
        omResponse.build(), purgeRequests, ozoneManager.isRatisEnabled(),
            getBucketLayout(), volBucketInfoMap, fromSnapshotInfo);

    return omClientResponse;
  }
}
