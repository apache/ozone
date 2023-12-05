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

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.impl.ReconNamespaceSummaryManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.client.ReplicationConfig.parse;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_DEFAULT;

/**
 * Manages records in the Deleted Directory Table, updating counts and sizes of
 * pending Directory Deletions in the backend.
 */
public class DeletedDirectoriesInsightHandler implements OmTableHandler {

  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private int replicationFactor;
  private OzoneConfiguration conf;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedKeysInsightHandler.class);

  public DeletedDirectoriesInsightHandler(
      ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager) {
    this.conf = new OzoneConfiguration();
    this.replicationFactor = getReplicationFactor();
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  /**
   * Invoked by the process method to add information on those directories that
   * have been backlogged in the backend for deletion.
   */
  @Override
  public void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                             String tableName,
                             Collection<String> sizeRelatedTables,
                             HashMap<String, Long> objectCountMap,
                             HashMap<String, Long> unreplicatedSizeMap,
                             HashMap<String, Long> replicatedSizeMap) {
    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);

      try {
        // Attempt to fetch size for deleted directory
        Long newDeletedDirectorySize =
            fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());

        unreplicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
            (k, size) -> size + newDeletedDirectorySize);
        replicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
            (k, size) -> size + newDeletedDirectorySize * replicationFactor);

      } catch (IOException e) {
        LOG.error(
            "IOException occurred while fetching size for deleted dir: {}",
            event.getKey());
      }
    } else {
      LOG.warn("Put event does not contain Key Info for {}.", event.getKey());
    }
  }

  /**
   * Invoked by the process method to remove information on those directories
   * that have been successfully deleted from the backend.
   */
  @Override
  public void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Collection<String> sizeRelatedTables,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unreplicatedSizeMap,
                                HashMap<String, Long> replicatedSizeMap) {

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count - 1L);
      try {
        // Attempt to fetch size for deleted directory
        Long newDeletedDirectorySize =
            fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());

        unreplicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
            (k, size) -> size > newDeletedDirectorySize ?
                size - newDeletedDirectorySize : 0L);
        replicatedSizeMap.computeIfPresent(unReplicatedSizeKey,
            (k, size) -> size > newDeletedDirectorySize * replicationFactor ?
                size - newDeletedDirectorySize * replicationFactor : 0L);

      } catch (IOException e) {
        LOG.error(
            "IOException occurred while fetching size for deleted dir: {}",
            event.getKey());
      }
    } else {
      LOG.warn("Delete event does not contain Key Info for {}.",
          event.getKey());
    }
  }

  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Collection<String> sizeRelatedTables,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unReplicatedSizeMap,
                                HashMap<String, Long> replicatedSizeMap) {
    // The size of deleted directories cannot change hence no-op.
    return;
  }

  /**
   * Invoked by the reprocess method to calculate the records count of the
   * deleted directories and their sizes.
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
          unReplicatedSize +=
              fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());
          count++;
        }
      }
    }
    replicatedSize = unReplicatedSize * replicationFactor;
    return Triple.of(count, unReplicatedSize, replicatedSize);
  }

  /**
   * Given an object ID, return total data size (no replication)
   * under this object. Note:- This method is RECURSIVE.
   *
   * @param objectId the object's ID
   * @return total used data size in bytes
   * @throws IOException ioEx
   */
  protected long fetchSizeForDeletedDirectory(long objectId)
      throws IOException {
    // Iterate the NSSummary table.
    Table<Long, NSSummary> summaryTable =
        reconNamespaceSummaryManager.getNSSummaryTable();
    Map<Long, NSSummary> summaryMap = new HashMap<>();

    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(objectId);
    if (nsSummary == null) {
      return 0L;
    }
    long totalSize = nsSummary.getSizeOfFiles();
    for (long childId : nsSummary.getChildDir()) {
      totalSize += fetchSizeForDeletedDirectory(childId);
    }
    return totalSize;
  }

  /**
   * Retrieves the replication factor from the configuration.
   *
   * This method reads the replication factor configuration value from the
   * system's configuration and returns it as an integer.
   *
   * @return The replication factor specified in the configuration.
   */
  protected int getReplicationFactor() {
    String replication = conf.get(OZONE_REPLICATION, OZONE_REPLICATION_DEFAULT);
    ReplicationConfig replicationConfig = parse(null, replication, conf);
    return replicationConfig.getRequiredNodes();
  }

  public static String getTableCountKeyFromTable(String tableName) {
    return tableName + "Count";
  }

  public static String getReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "ReplicatedDataSize";
  }

  public static String getUnReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "UnReplicatedDataSize";
  }
}
