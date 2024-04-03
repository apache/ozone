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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Manages records in the Deleted Table, updating counts and sizes of
 * pending Key Deletions in the backend.
 */
public class DeletedKeysInsightHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedKeysInsightHandler.class);

  /**
   * Invoked by the process method to add information on those keys that have
   * been backlogged in the backend for deletion.
   */
  @Override
  public void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                             String tableName,
                             HashMap<String, Long> objectCountMap,
                             HashMap<String, Long> unReplicatedSizeMap,
                             HashMap<String, Long> replicatedSizeMap) {

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count + repeatedOmKeyInfo.getOmKeyInfoList().size());
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unReplicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + result.getLeft());
      replicatedSizeMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size + result.getRight());
    } else {
      LOG.warn("Put event does not have the Key Info for {}.",
          event.getKey());
    }

  }

  /**
   * Invoked by the process method to remove information on those keys that have
   * been successfully deleted from the backend.
   */
  @Override
  public void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unReplicatedSizeMap,
                                HashMap<String, Long> replicatedSizeMap) {

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) ->
          count > 0 ? count - repeatedOmKeyInfo.getOmKeyInfoList().size() : 0L);
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unReplicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > result.getLeft() ? size - result.getLeft() : 0L);
      replicatedSizeMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size > result.getRight() ? size - result.getRight() :
              0L);
    } else {
      LOG.warn("Delete event does not have the Key Info for {}.",
          event.getKey());
    }
  }

  /**
   * Invoked by the process method to update the statistics on the keys
   * pending to be deleted.
   */
  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unReplicatedSizeMap,
                                HashMap<String, Long> replicatedSizeMap) {
    // The size of deleted keys cannot change hence no-op.
    return;
  }

  /**
   * Invoked by the reprocess method to calculate the records count of the
   * deleted table and the sizes of replicated and unreplicated keys that are
   * pending deletion in Ozone.
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
          RepeatedOmKeyInfo repeatedOmKeyInfo = (RepeatedOmKeyInfo) kv
              .getValue();
          Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
          unReplicatedSize += result.getRight();
          replicatedSize += result.getLeft();
          count += repeatedOmKeyInfo.getOmKeyInfoList().size();
        }
      }
    }
    return Triple.of(count, unReplicatedSize, replicatedSize);
  }
}
