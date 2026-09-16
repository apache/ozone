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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.ReconBasicOmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages records in the MultipartInfo Table, updating counts and sizes of
 * multipart upload keys in the backend.
 *
 * <p>Multipart uploads are stored in one of two schemas:
 * <ul>
 *   <li>Legacy (schemaVersion {@link OmMultipartKeyInfo#LEGACY_SCHEMA_VERSION}):
 *   the part information is embedded inside the {@code multipartInfoTable} value
 *   (see {@link OmMultipartKeyInfo#getPartKeyInfoMap()}).</li>
 *   <li>Split parts-table (schemaVersion
 *   {@link OmMultipartKeyInfo#SPLIT_PARTS_TABLE_SCHEMA_VERSION}): the
 *   {@code multipartInfoTable} value carries no embedded parts; each part is a
 *   separate row in the {@code multipartPartsTable}, keyed by
 *   {@code uploadId/partNumber}.</li>
 * </ul>
 *
 * <p>The event handlers below only access {@code multipartInfoTable} events,
 * whose values embed parts only for the legacy schema. Split-schema part
 * sizes therefore cannot be accounted incrementally from these events (the
 * handler has no DB access and the event carries no part data).
 * So they are reconciled during the periodic reprocess in {@link #getTableSizeAndCount(String, OMMetadataManager)},
 * which reads the split {@code multipartPartsTable} directly.
 */
public class MultipartInfoInsightHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(MultipartInfoInsightHandler.class);

  /**
   * Consumes the (unreplicated, replicated) size of a single multipart part.
   */
  @FunctionalInterface
  private interface PartSizeConsumer {
    void accept(long dataSize, long replicatedSize);
  }

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
      applyLegacyPartSizes(multipartKeyInfo, tableName, unReplicatedSizeMap, replicatedSizeMap, true);
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
      applyLegacyPartSizes(multipartKeyInfo, tableName, unReplicatedSizeMap, replicatedSizeMap, false);
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

      // In an Update event the count for the multipart info table does not
      // change, so only the sizes are adjusted: subtract the old parts and add
      // the new parts.
      OmMultipartKeyInfo oldMultipartKeyInfo = (OmMultipartKeyInfo) event.getOldValue();
      OmMultipartKeyInfo newMultipartKeyInfo = (OmMultipartKeyInfo) event.getValue();
      applyLegacyPartSizes(oldMultipartKeyInfo, tableName, unReplicatedSizeMap, replicatedSizeMap, false);
      applyLegacyPartSizes(newMultipartKeyInfo, tableName, unReplicatedSizeMap, replicatedSizeMap, true);
    } else {
      LOG.warn("Update event does not have the Multipart Key Info for {}.", event.getKey());
    }
  }

  /**
   * This method is called by the reprocess method. It calculates the record
   * counts for the multipart info table. Additionally, it computes the sizes
   * of both replicated and unreplicated parts that are currently in multipart
   * uploads in the backend.
   *
   * <p>This is schema-aware: legacy (schemaVersion 0) part sizes are read from
   * the embedded {@link OmMultipartKeyInfo#getPartKeyInfoMap()}, while split
   * (schemaVersion 1) part sizes are summed from the separate
   * {@code multipartPartsTable}. The count returned is always the number of
   * multipart uploads (rows in the {@code multipartInfoTable}), regardless of
   * schema.
   */
  @Override
  public Triple<Long, Long, Long> getTableSizeAndCount(String tableName,
      OMMetadataManager omMetadataManager) throws IOException {
    long count = 0;
    // left = unreplicated size, right = replicated size. A mutable pair is used
    // so the running totals can be updated from the part-iteration lambda below.
    final MutablePair<Long, Long> sizes = MutablePair.of(0L, 0L);

    // uploadId -> parent replication config, for split-schema MPUs. Their parts
    // live in multipartPartsTable but do not carry a replication config, so the
    // parent's config (recorded here) is used to compute their replicated size
    // in the second pass below.
    Map<String, ReplicationConfig> splitSchemaUploads = new HashMap<>();

    Table<String, OmMultipartKeyInfo> table =
        (Table<String, OmMultipartKeyInfo>) omMetadataManager.getTable(tableName);
    try (TableIterator<String, Table.KeyValue<String, OmMultipartKeyInfo>> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmMultipartKeyInfo> kv = iterator.next();
        if (kv != null && kv.getValue() != null) {
          OmMultipartKeyInfo multipartKeyInfo = kv.getValue();
          if (isLegacySchema(multipartKeyInfo)) {
            forEachLegacyPart(multipartKeyInfo, (dataSize, replicatedSize) -> {
              sizes.setLeft(sizes.getLeft() + dataSize);
              sizes.setRight(sizes.getRight() + replicatedSize);
            });
          } else {
            // Split schema: parts are stored in multipartPartsTable. Remember
            // the parent replication config keyed by uploadId (last component
            // of the multipart key) for the second pass.
            splitSchemaUploads.put(getUploadIdFromMultipartKey(kv.getKey()),
                multipartKeyInfo.getReplicationConfig());
          }
          count++;
        }
      }
    }

    // Second pass: sum the sizes of split-schema parts from multipartPartsTable.
    if (!splitSchemaUploads.isEmpty()) {
      Table<OmMultipartPartKey, OmMultipartPartInfo> partsTable =
          omMetadataManager.getMultipartPartsTable();
      try (TableIterator<OmMultipartPartKey, Table.KeyValue<OmMultipartPartKey, OmMultipartPartInfo>> partIterator =
               partsTable.iterator()) {
        while (partIterator.hasNext()) {
          Table.KeyValue<OmMultipartPartKey, OmMultipartPartInfo> kv = partIterator.next();
          if (kv != null && kv.getKey() != null && kv.getValue() != null) {
            ReplicationConfig replicationConfig = splitSchemaUploads.get(kv.getKey().getUploadId());
            if (replicationConfig == null) {
              // Part whose parent MPU is not in multipartInfoTable (e.g. an
              // orphan mid-cleanup). Skip to avoid mis-attributing its size.
              continue;
            }
            long partDataSize = kv.getValue().getDataSize();
            sizes.setLeft(sizes.getLeft() + partDataSize);
            sizes.setRight(sizes.getRight()
                + QuotaUtil.getReplicatedSize(partDataSize, replicationConfig));
          }
        }
      }
    }

    return Triple.of(count, sizes.getLeft(), sizes.getRight());
  }

  /**
   * Adds (or subtracts) the sizes of a legacy MPU's embedded parts to the size
   * maps. Split-schema MPUs carry no embedded parts and are ignored here (their
   * sizes are reconciled during reprocess; see class Javadoc).
   *
   * @param add {@code true} to add the part sizes (PUT / new value in UPDATE),
   *            {@code false} to subtract them (DELETE / old value in UPDATE).
   */
  private void applyLegacyPartSizes(OmMultipartKeyInfo multipartKeyInfo, String tableName,
      Map<String, Long> unReplicatedSizeMap, Map<String, Long> replicatedSizeMap, boolean add) {
    forEachLegacyPart(multipartKeyInfo, (dataSize, replicatedSize) -> {
      updateSize(unReplicatedSizeMap, getUnReplicatedSizeKeyFromTable(tableName), dataSize, add, "unreplicated");
      updateSize(replicatedSizeMap, getReplicatedSizeKeyFromTable(tableName), replicatedSize, add, "replicated");
    });
  }

  /**
   * Invokes {@code consumer} with the (unreplicated, replicated) size of each
   * embedded part of a legacy MPU. Does nothing for split-schema MPUs.
   */
  private static void forEachLegacyPart(OmMultipartKeyInfo multipartKeyInfo, PartSizeConsumer consumer) {
    if (!isLegacySchema(multipartKeyInfo)) {
      return;
    }
    for (PartKeyInfo partKeyInfo : multipartKeyInfo.getPartKeyInfoMap()) {
      ReconBasicOmKeyInfo omKeyInfo = ReconBasicOmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
      consumer.accept(omKeyInfo.getDataSize(), omKeyInfo.getReplicatedSize());
    }
  }

  /**
   * Adds or subtracts {@code delta} from the value stored under {@code key} in
   * {@code sizeMap} (only if the key is already present). Subtraction is clamped
   * at zero, and an underflow is logged (as it indicates an accounting anomaly,
   * e.g. a delete/update for a part whose size was never added).
   *
   * @param sizeType a human-readable label ("unreplicated"/"replicated") used
   *                 only in the underflow warning message.
   */
  private static void updateSize(Map<String, Long> sizeMap, String key, long delta, boolean add, String sizeType) {
    sizeMap.computeIfPresent(key, (k, size) -> {
      if (add) {
        return size + delta;
      }
      if (size < delta) {
        LOG.warn("Negative {} size for key: {}. Current: {}, Part: {}. Clamping to 0.",
            sizeType, k, size, delta);
        return 0L;
      }
      return size - delta;
    });
  }

  private static boolean isLegacySchema(OmMultipartKeyInfo multipartKeyInfo) {
    return multipartKeyInfo.getSchemaVersion() == OmMultipartKeyInfo.LEGACY_SCHEMA_VERSION;
  }

  /**
   * The multipart key is {@code .../uploadId}; the split parts-table rows are
   * keyed by that same uploadId. Extract it from the last path component.
   */
  private static String getUploadIdFromMultipartKey(String multipartKey) {
    int idx = multipartKey.lastIndexOf(OzoneConsts.OM_KEY_PREFIX);
    return idx >= 0 ? multipartKey.substring(idx + OzoneConsts.OM_KEY_PREFIX.length()) : multipartKey;
  }
}
