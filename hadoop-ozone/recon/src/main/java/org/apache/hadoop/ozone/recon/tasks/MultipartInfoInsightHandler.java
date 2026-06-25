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
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.ReconBasicOmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages records in the MultipartInfo Table, updating counts and sizes of
 * multipart upload keys in the backend.
 */
public class MultipartInfoInsightHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartInfoInsightHandler.class);

  /**
   * Invoked by the process method to add information on those keys that have
   * been initiated for multipart upload in the backend.
   */
  @Override
  public void handlePutEvent(OMDBUpdateEvent<String, Object> event, String tableName, Map<String, Long> objectCountMap,
      Map<String, Long> unReplicatedSizeMap, Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      OmMultipartKeyInfo multipartKeyInfo = (OmMultipartKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(getTableCountKeyFromTable(tableName),
          (k, count) -> count + 1L);

      for (PartKeyInfo partKeyInfo : multipartKeyInfo.getPartKeyInfoMap()) {
        ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
        unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size + omKeyInfo.getDataSize());
        replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size + omKeyInfo.getReplicatedSize());
      }
    } else {
      LOG.warn("Put event does not have the Multipart Key Info for {}.", event.getKey());
    }
  }

  /**
   * Invoked by the process method to delete information on those multipart uploads that
   * have been completed or aborted in the backend.
   */
  @Override
  public void handleDeleteEvent(OMDBUpdateEvent<String, Object> event, String tableName,
      Map<String, Long> objectCountMap, Map<String, Long> unReplicatedSizeMap, Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      OmMultipartKeyInfo multipartKeyInfo = (OmMultipartKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(getTableCountKeyFromTable(tableName),
          (k, count) -> count > 0 ? count - 1L : 0L);

      for (PartKeyInfo partKeyInfo : multipartKeyInfo.getPartKeyInfoMap()) {
        ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
        unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
            (k, size) -> {
              long newSize = size > omKeyInfo.getDataSize() ? size - omKeyInfo.getDataSize() : 0L;
              if (newSize < 0) {
                LOG.warn("Negative unreplicated size for key: {}. Original: {}, Part: {}",
                    k, size, omKeyInfo.getDataSize());
              }
              return newSize;
            });
        replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
            (k, size) -> {
              long newSize = size > omKeyInfo.getReplicatedSize() ? size - omKeyInfo.getReplicatedSize() : 0L;
              if (newSize < 0) {
                LOG.warn("Negative replicated size for key: {}. Original: {}, Part: {}",
                    k, size, omKeyInfo.getReplicatedSize());
              }
              return newSize;
            });
      }
    } else {
      LOG.warn("Delete event does not have the Multipart Key Info for {}.", event.getKey());
    }
  }

  /**
   * Invoked by the process method to update information on those multipart uploads that
   * have been updated in the backend.
   */
  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event, String tableName,
      Map<String, Long> objectCountMap, Map<String, Long> unReplicatedSizeMap, Map<String, Long> replicatedSizeMap) {

    if (event.getValue() != null) {
      if (event.getOldValue() == null) {
        LOG.warn("Update event does not have the old Multipart Key Info for {}.", event.getKey());
        return;
      }

      // In Update event the count for the multipart info table will not change. So we
      // don't need to update the count.
      OmMultipartKeyInfo oldMultipartKeyInfo = (OmMultipartKeyInfo) event.getOldValue();
      OmMultipartKeyInfo newMultipartKeyInfo = (OmMultipartKeyInfo) event.getValue();

      // Calculate old sizes
      for (PartKeyInfo partKeyInfo : oldMultipartKeyInfo.getPartKeyInfoMap()) {
        ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
        unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size - omKeyInfo.getDataSize());
        replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size - omKeyInfo.getReplicatedSize());
      }

      // Calculate new sizes
      for (PartKeyInfo partKeyInfo : newMultipartKeyInfo.getPartKeyInfoMap()) {
        ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
        unReplicatedSizeMap.computeIfPresent(getUnReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size + omKeyInfo.getDataSize());
        replicatedSizeMap.computeIfPresent(getReplicatedSizeKeyFromTable(tableName),
            (k, size) -> size + omKeyInfo.getReplicatedSize());
      }
    } else {
      LOG.warn("Update event does not have the Multipart Key Info for {}.", event.getKey());
    }
  }

  /**
   * This method is called by the reprocess method. It calculates the record
   * counts for the multipart info table. Additionally, it computes the sizes
   * of both replicated and unreplicated parts that are currently in multipart
   * uploads in the backend.
   */
  @Override
  public Triple<Long, Long, Long> getTableSizeAndCount(String tableName,
      OMMetadataManager omMetadataManager) throws IOException {
    long count = 0;
    long unReplicatedSize = 0;
    long replicatedSize = 0;

    Table<String, OmMultipartKeyInfo> table =
        (Table<String, OmMultipartKeyInfo>) omMetadataManager.getTable(tableName);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmMultipartKeyInfo>> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmMultipartKeyInfo> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          OmMultipartKeyInfo multipartKeyInfo = kv.getValue();
          for (PartKeyInfo partKeyInfo : multipartKeyInfo.getPartKeyInfoMap()) {
            ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
            unReplicatedSize += omKeyInfo.getDataSize();
            replicatedSize += omKeyInfo.getReplicatedSize();
          }
          count++;
        }
      }
    }
    return Triple.of(count, unReplicatedSize, replicatedSize);
  }
}
