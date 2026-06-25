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
import org.apache.hadoop.ozone.om.OMMetadataManager;

/**
 * Interface for handling PUT, DELETE and UPDATE events for size-related
 * tables for OM Insights.
 */
public interface OmTableHandler {

  /**
   * Handles a PUT event for size-related tables by updating both the data
   * sizes and their corresponding record counts in the tables.
   *
   * @param event                    The PUT event to be processed.
   * @param tableName                Table name associated with the event.
   * @param objectCountMap           A map storing object counts.
   * @param unReplicatedSizeMap A map storing unReplicated size counts.
   * @param replicatedSizeMap   A map storing replicated size counts.
   */
  void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                      String tableName,
                      Map<String, Long> objectCountMap,
                      Map<String, Long> unReplicatedSizeMap,
                      Map<String, Long> replicatedSizeMap);


  /**
   * Handles a DELETE event for size-related tables by updating both the data
   * sizes and their corresponding record counts in the tables.
   *
   * @param event                    The DELETE event to be processed.
   * @param tableName                Table name associated with the event.
   * @param objectCountMap           A map storing object counts.
   * @param unReplicatedSizeMap A map storing unReplicated size counts.
   * @param replicatedSizeMap   A map storing replicated size counts.
   */
  void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Map<String, Long> objectCountMap,
                         Map<String, Long> unReplicatedSizeMap,
                         Map<String, Long> replicatedSizeMap);


  /**
   * Handles an UPDATE event for size-related tables by updating both the data
   * sizes and their corresponding record counts in the tables.
   *
   * @param event                    The UPDATE event to be processed.
   * @param tableName                Table name associated with the event.
   * @param objectCountMap           A map storing object counts.
   * @param unReplicatedSizeMap A map storing unReplicated size counts.
   * @param replicatedSizeMap   A map storing replicated size counts.
   */
  void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Map<String, Long> objectCountMap,
                         Map<String, Long> unReplicatedSizeMap,
                         Map<String, Long> replicatedSizeMap);


  /**
   * Returns a triple with the total count of records (left), total unreplicated
   * size (middle), and total replicated size (right) for the given table.
   * Increments count for each record and adds the dataSize if a record's value
   * is an instance of OmKeyInfo,RepeatedOmKeyInfo.
   *
   * @param tableName The name of the table to process.
   * @param omMetadataManager The OM metadata manager to get the table.
   * @return A Triple with three Long values representing the count,
   * unReplicated size and replicated size.
   * @throws IOException If an I/O error occurs during the iterator traversal.
   */
  Triple<Long, Long, Long> getTableSizeAndCount(String tableName, 
      OMMetadataManager omMetadataManager) throws IOException;

  /**
   * Returns the count key for the given table.
   *
   * @param tableName The name of the table.
   * @return The count key for the table.
   */
  default String getTableCountKeyFromTable(String tableName) {
    return tableName + "Count";
  }

  /**
   * Returns the replicated size key for the given table.
   *
   * @param tableName The name of the table.
   * @return The replicated size key for the table.
   */
  default String getReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "ReplicatedDataSize";
  }

  /**
   * Returns the unreplicated size key for the given table.
   *
   * @param tableName The name of the table.
   * @return The unreplicated size key for the table.
   */
  default String getUnReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "UnReplicatedDataSize";
  }
}
