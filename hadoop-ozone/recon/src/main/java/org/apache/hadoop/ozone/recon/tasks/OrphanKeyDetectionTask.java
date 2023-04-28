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

package org.apache.hadoop.ozone.recon.tasks;

import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Task class to iterate over the OM DB and detect the orphan keys metadata.
 */
public class OrphanKeyDetectionTask implements ReconDiagnosticTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(OrphanKeyDetectionTask.class);

  private OMMetadataManager omMetadataManager;

  @Inject
  public OrphanKeyDetectionTask(OMMetadataManager
                                      omMetadataManager) {
    this.omMetadataManager = omMetadataManager;
  }

  @Override
  public String getTaskName() {
    return "OrphanKeyDetectionTask";
  }

  /**
   * trigger the orphan key detection diagnostic task.
   *
   * @param cls
   * @return Pair of task name -> task success.
   */
  @Override
  public <T> Pair<String, Boolean> trigger(T cls) {
    long omKeyCount = 0;
    List<OmKeyInfo> orphanKeys = new ArrayList<>();
    try {
      LOG.info("Starting OrphanKeyDetectionTask...");
      Instant start = Instant.now();

      Table<String, OmKeyInfo> omKeyInfoTable =
          omMetadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      try (
          TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
              keyIter = omKeyInfoTable.iterator()) {
        while (keyIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
          OmKeyInfo omKeyInfo = kv.getValue();
          if(isFileOrphan(omKeyInfo)) {
            orphanKeys.add(omKeyInfo);
          }
          omKeyCount++;
        }
      }
      LOG.info("Completed OrphanKeyDetectionTask.");
      Instant end = Instant.now();
      long duration = Duration.between(start, end).toMillis();
      LOG.info("It took me {} seconds to process {} keys.",
          (double) duration / 1000.0, omKeyCount);
    } catch (IOException ioEx) {
      LOG.error("Unable to detect orphan keys metadata. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    /*try {
      //writeToTheDB(containerKeyMap, containerKeyCountMap, deletedKeyCountList);
    } catch (IOException e) {
      LOG.error("Unable to write orphan keys metadata in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }*/
    return new ImmutablePair<>(getTaskName(), true);
  }

  /*private void writeToTheDB(Map<ContainerKeyPrefix, Integer> containerKeyMap,
                            Map<Long, Long> containerKeyCountMap,
                            List<ContainerKeyPrefix> deletedContainerKeyList)
      throws IOException {
    try (RDBBatchOperation rdbBatchOperation = new RDBBatchOperation()) {
      containerKeyMap.keySet().forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyMapping(rdbBatchOperation, key,
                  containerKeyMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });
      containerKeyCountMap.keySet().forEach((Long key) -> {
        try {
          reconContainerMetadataManager
              .batchStoreContainerKeyCounts(rdbBatchOperation, key,
                  containerKeyCountMap.get(key));
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });

      deletedContainerKeyList.forEach((ContainerKeyPrefix key) -> {
        try {
          reconContainerMetadataManager
              .batchDeleteContainerMapping(rdbBatchOperation, key);
        } catch (IOException e) {
          LOG.error("Unable to write Container Key Prefix data in Recon DB.",
              e);
        }
      });
      reconContainerMetadataManager.commitBatchOperation(rdbBatchOperation);
    }
  }*/

  public boolean isFileOrphan(OmKeyInfo omKeyInfo)
      throws IOException {
    long lastKnownParentId = omKeyInfo.getParentObjectID();
    OmDirectoryInfo lastFoundDirNode = iterateParentDirs(lastKnownParentId);
    long bucketObjectId =
        getBucketObjectId(omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    if (bucketObjectId == -1) {
      return true;
    }
    lastKnownParentId =
        (null != lastFoundDirNode ? lastFoundDirNode.getParentObjectID() : -1);
    if (bucketObjectId != lastKnownParentId) {
      return true;
    }
    if (getVolumeObjectId(omKeyInfo.getVolumeName()) == -1) {
      return true;
    }
    return false;
  }

  private OmDirectoryInfo iterateParentDirs(long lastKnownParentId)
      throws IOException {
    boolean found = false;
    OmDirectoryInfo omDirectoryInfo = null;
    try (
        TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
            dirIter = omMetadataManager.getDirectoryTable().iterator()) {
      while (dirIter.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> nextDirNodeKeyVal =
            dirIter.next();
        OmDirectoryInfo dirNode = nextDirNodeKeyVal.getValue();
        if (dirNode.getObjectID() == lastKnownParentId) {
          lastKnownParentId = dirNode.getParentObjectID();
          omDirectoryInfo = dirNode;
          found = true;
          break;
        }
      }
      if (found) {
        iterateParentDirs(lastKnownParentId);
      }
    }
    return omDirectoryInfo;
  }

  /**
   * Given a volume name, get the volume object ID.
   * @param volName volume name
   * @return volume objectID
   * @throws IOException
   */
  public long getVolumeObjectId(String volName) throws IOException {
    String volumeKey = omMetadataManager.getVolumeKey(volName);
    OmVolumeArgs volumeInfo = omMetadataManager
        .getVolumeTable().getSkipCache(volumeKey);
    return (null != volumeInfo ? volumeInfo.getObjectID() : -1);
  }

  /**
   * Given a bucket name, get the bucket object ID.
   * @param volName volume name
   * @param bucketName bucket name
   * @return bucket objectID
   * @throws IOException
   */
  public long getBucketObjectId(String volName, String bucketName)
      throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volName, bucketName);
    OmBucketInfo bucketInfo = omMetadataManager
        .getBucketTable().getSkipCache(bucketKey);
    return (null != bucketInfo ? bucketInfo.getObjectID() : -1);
  }

}
