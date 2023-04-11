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
package org.apache.hadoop.ozone.om.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Abstract's KeyDeletingService.
 */
public abstract class AbstractKeyDeletingService extends BackgroundService {

  private final OzoneManager ozoneManager;
  private final ScmBlockLocationProtocol scmClient;
  private static ClientId clientId = ClientId.randomId();
  private final AtomicLong runCount;

  public AbstractKeyDeletingService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      OzoneManager ozoneManager, ScmBlockLocationProtocol scmClient) {
    super(serviceName, interval, unit, threadPoolSize, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.scmClient = scmClient;
    this.runCount = new AtomicLong(0);
  }

  protected int processKeyDeletes(List<BlockGroup> keyBlocksList,
                                  KeyManager manager,
                                  String snapTableKey) throws IOException {

    long startTime = Time.monotonicNow();
    int delCount = 0;
    List<DeleteBlockGroupResult> blockDeletionResults =
        scmClient.deleteKeyBlocks(keyBlocksList);
    if (blockDeletionResults != null) {
      if (isRatisEnabled()) {
        delCount = submitPurgeKeysRequest(blockDeletionResults, snapTableKey);
      } else {
        // TODO: Once HA and non-HA paths are merged, we should have
        //  only one code path here. Purge keys should go through an
        //  OMRequest model.
        delCount = deleteAllKeys(blockDeletionResults, manager);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Blocks for {} (out of {}) keys are deleted in {} ms",
            delCount, blockDeletionResults.size(),
            Time.monotonicNow() - startTime);
      }
    }
    return delCount;
  }

  /**
   * Deletes all the keys that SCM has acknowledged and queued for delete.
   *
   * @param results DeleteBlockGroups returned by SCM.
   * @throws IOException      on Error
   */
  private int deleteAllKeys(List<DeleteBlockGroupResult> results,
      KeyManager manager) throws IOException {
    Table<String, RepeatedOmKeyInfo> deletedTable =
        manager.getMetadataManager().getDeletedTable();
    DBStore store = manager.getMetadataManager().getStore();

    // Put all keys to delete in a single transaction and call for delete.
    int deletedCount = 0;
    try (BatchOperation writeBatch = store.initBatchOperation()) {
      for (DeleteBlockGroupResult result : results) {
        if (result.isSuccess()) {
          // Purge key from OM DB.
          deletedTable.deleteWithBatch(writeBatch,
              result.getObjectKey());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Key {} deleted from OM DB", result.getObjectKey());
          }
          deletedCount++;
        }
      }
      // Write a single transaction for delete.
      store.commitBatchOperation(writeBatch);
    }
    return deletedCount;
  }

  /**
   * Submits PurgeKeys request for the keys whose blocks have been deleted
   * by SCM.
   * @param results DeleteBlockGroups returned by SCM.
   */
  private int submitPurgeKeysRequest(List<DeleteBlockGroupResult> results,
                                     String snapTableKey) {
    Map<Pair<String, String>, List<String>> purgeKeysMapPerBucket =
        new HashMap<>();

    // Put all keys to be purged in a list
    int deletedCount = 0;
    for (DeleteBlockGroupResult result : results) {
      if (result.isSuccess()) {
        // Add key to PurgeKeys list.
        String deletedKey = result.getObjectKey();
        // Parse Volume and BucketName
        addToMap(purgeKeysMapPerBucket, deletedKey);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Key {} set to be purged from OM DB", deletedKey);
        }
        deletedCount++;
      }
    }

    PurgeKeysRequest.Builder purgeKeysRequest = PurgeKeysRequest.newBuilder();
    if (snapTableKey != null) {
      purgeKeysRequest.setSnapshotTableKey(snapTableKey);
    }

    // Add keys to PurgeKeysRequest bucket wise.
    for (Map.Entry<Pair<String, String>, List<String>> entry :
        purgeKeysMapPerBucket.entrySet()) {
      Pair<String, String> volumeBucketPair = entry.getKey();
      DeletedKeys deletedKeysInBucket = DeletedKeys.newBuilder()
          .setVolumeName(volumeBucketPair.getLeft())
          .setBucketName(volumeBucketPair.getRight())
          .addAllKeys(entry.getValue())
          .build();
      purgeKeysRequest.addDeletedKeys(deletedKeysInBucket);
    }

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(Type.PurgeKeys)
        .setPurgeKeysRequest(purgeKeysRequest)
        .setClientId(clientId.toString())
        .build();

    // Submit PurgeKeys request to OM
    try {
      RaftClientRequest raftClientRequest =
          createRaftClientRequestForPurge(omRequest);
      ozoneManager.getOmRatisServer().submitRequest(omRequest,
          raftClientRequest);
    } catch (ServiceException e) {
      LOG.error("PurgeKey request failed. Will retry at next run.");
      return 0;
    }

    return deletedCount;
  }

  private RaftClientRequest createRaftClientRequestForPurge(
      OMRequest omRequest) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(ozoneManager.getOmRatisServer().getRaftPeerId())
        .setGroupId(ozoneManager.getOmRatisServer().getRaftGroupId())
        .setCallId(runCount.get())
        .setMessage(
            Message.valueOf(
                OMRatisHelper.convertRequestToByteString(omRequest)))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  /**
   * Parse Volume and Bucket Name from ObjectKey and add it to given map of
   * keys to be purged per bucket.
   */
  private void addToMap(Map<Pair<String, String>, List<String>> map,
                        String objectKey) {
    // Parse volume and bucket name
    String[] split = objectKey.split(OM_KEY_PREFIX);
    Preconditions.assertTrue(split.length > 3, "Volume and/or Bucket Name " +
        "missing from Key Name.");
    Pair<String, String> volumeBucketPair = Pair.of(split[1], split[2]);
    if (!map.containsKey(volumeBucketPair)) {
      map.put(volumeBucketPair, new ArrayList<>());
    }
    map.get(volumeBucketPair).add(objectKey);
  }

  public boolean isRatisEnabled() {
    if (ozoneManager == null) {
      return false;
    }
    return ozoneManager.isRatisEnabled();
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  public ScmBlockLocationProtocol getScmClient() {
    return scmClient;
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }
}
