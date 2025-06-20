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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages records in the key table and file table - updating counts, both replicated and unreplicated sizes of
 * committed keys in the backend.
 */
public class CommittedKeysInsightHandler implements OmTableHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(CommittedKeysInsightHandler.class);

  /**
   * Invoked by the process method to add information on those keys that have been committed in the OM DB.
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
   * Invoked by the process method to delete information on those keys that are no longer present in the OM DB.
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
   * Invoked by the process method to update information on those keys and files that have been updated in the OM DB.
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

      // During an UPDATE event the count for the key table and file table will not change. So we don't have to
      // update the count.
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
   * This method is called by the reprocess method. It calculates the record counts for both the key table and the
   * file table. Additionally, it computes both replicated and unreplicated sizes of keys that are currently
   * committed in the OM DB.
   */
  @Override
  public Triple<Long, Long, Long> getTableSizeAndCount(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator)
      throws IOException {
    long count = 0;
    long unReplicatedSize = 0;
    long replicatedSize = 0;

    if (iterator != null) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, ?> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          OmKeyInfo omKeyInfo = (OmKeyInfo) kv.getValue();
          unReplicatedSize += omKeyInfo.getDataSize();
          replicatedSize += omKeyInfo.getReplicatedSize();
          count++;
        }
      }
    }
    return Triple.of(count, unReplicatedSize, replicatedSize);
  }

}
