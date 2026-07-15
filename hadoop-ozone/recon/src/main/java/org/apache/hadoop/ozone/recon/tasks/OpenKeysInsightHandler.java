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

package org.apache.hadoop.ozone.recon.tasks;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages records in the OpenKey Table, updating counts and sizes of
 * open keys in the backend.
 */
public class OpenKeysInsightHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenKeysInsightHandler.class);

  /**
   * Invoked by the process method to add information on those keys that have
   * been open in the backend.
   */
  @Override
  public void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                             String tableName,
                             Map<String, Long> objectCountMap,
                             Map<String, Long> unReplicatedSizeMap,
                             Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(getTableCountKeyFromTable(tableName), (k, count) -> count + 1L);
      unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size + omKeyInfo.getDataSize());
      replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size + omKeyInfo.getReplicatedSize());
    } else {
      LOG.warn("Put event does not have the Key Info for {}.",
          event.getKey());
    }
  }

  /**
   * Invoked by the process method to delete information on those keys that are
   * no longer closed in the backend.
   */
  @Override
  public void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Map<String, Long> objectCountMap,
                                Map<String, Long> unReplicatedSizeMap,
                                Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(getTableCountKeyFromTable(tableName),
          (k, count) -> count > 0 ? count - 1L : 0L);
      unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size > omKeyInfo.getDataSize() ?
              size - omKeyInfo.getDataSize() : 0L);
      replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size > omKeyInfo.getReplicatedSize() ?
              size - omKeyInfo.getReplicatedSize() : 0L);
    } else {
      LOG.warn("Delete event does not have the Key Info for {}.",
          event.getKey());
    }
  }

  /**
   * Invoked by the process method to update information on those open keys that
   * have been updated in the backend.
   */
  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Map<String, Long> objectCountMap,
                                Map<String, Long> unReplicatedSizeMap,
                                Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      if (event.getOldValue() == null) {
        LOG.warn("Update event does not have the old Key Info for {}.",
            event.getKey());
        return;
      }

      // In Update event the count for the open table will not change. So we
      // don't need to update the count.
      OmKeyInfo oldKeyInfo = (OmKeyInfo) event.getOldValue();
      OmKeyInfo newKeyInfo = (OmKeyInfo) event.getValue();
      unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size - oldKeyInfo.getDataSize() +
              newKeyInfo.getDataSize());
      replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
          (k, size) -> size - oldKeyInfo.getReplicatedSize() +
              newKeyInfo.getReplicatedSize());
    } else {
      LOG.warn("Update event does not have the Key Info for {}.",
          event.getKey());
    }
  }

  /**
   * This method is called by the reprocess method. It calculates the record
   * counts for both the open key table and the open file table. Additionally,
   * it computes the sizes of both replicated and unreplicated keys
   * that are currently open in the backend.
   */
  @Override
  public Triple<Long, Long, Long> getTableSizeAndCount(String tableName,
      OMMetadataManager omMetadataManager) throws IOException {
    long count = 0;
    long unReplicatedSize = 0;
    long replicatedSize = 0;

    Table<String, OmKeyInfo> table = (Table<String, OmKeyInfo>) omMetadataManager.getTable(tableName);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          OmKeyInfo omKeyInfo = kv.getValue();
          unReplicatedSize += omKeyInfo.getDataSize();
          replicatedSize += omKeyInfo.getReplicatedSize();
          count++;
        }
      }
    }
    return Triple.of(count, unReplicatedSize, replicatedSize);
  }

}
