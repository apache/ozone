package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

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
   * @param sizeRelatedTables        Tables Requiring Size Calculation.
   * @param objectCountMap           A map storing object counts.
   * @param unreplicatedSizeCountMap A map storing unReplicated size counts.
   * @param replicatedSizeCountMap   A map storing replicated size counts.
   * @throws IOException If an I/O error occurs during processing.
   */
  void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                      String tableName,
                      Collection<String> sizeRelatedTables,
                      HashMap<String, Long> objectCountMap,
                      HashMap<String, Long> unreplicatedSizeCountMap,
                      HashMap<String, Long> replicatedSizeCountMap);
  /**
   * Handles a DELETE event for size-related tables by updating both the data
   * sizes and their corresponding record counts in the tables.
   *
   * @param event                    The DELETE event to be processed.
   * @param tableName                Table name associated with the event.
   * @param sizeRelatedTables        Tables Requiring Size Calculation.
   * @param objectCountMap           A map storing object counts.
   * @param unreplicatedSizeCountMap A map storing unReplicated size counts.
   * @param replicatedSizeCountMap   A map storing replicated size counts.
   * @throws IOException If an I/O error occurs during processing.
   */
  void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Collection<String> sizeRelatedTables,
                         HashMap<String, Long> objectCountMap,
                         HashMap<String, Long> unreplicatedSizeCountMap,
                         HashMap<String, Long> replicatedSizeCountMap);

  /**
   * Handles an UPDATE event for size-related tables by updating both the data
   * sizes and their corresponding record counts in the tables.
   *
   * @param event                    The UPDATE event to be processed.
   * @param tableName                Table name associated with the event.
   * @param sizeRelatedTables        Tables Requiring Size Calculation.
   * @param objectCountMap           A map storing object counts.
   * @param unreplicatedSizeCountMap A map storing unReplicated size counts.
   * @param replicatedSizeCountMap   A map storing replicated size counts.
   */
  void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                         String tableName,
                         Collection<String> sizeRelatedTables,
                         HashMap<String, Long> objectCountMap,
                         HashMap<String, Long> unreplicatedSizeCountMap,
                         HashMap<String, Long> replicatedSizeCountMap);


  /**
   * Returns a triple with the total count of records (left), total unreplicated
   * size (middle), and total replicated size (right) in the given iterator.
   * Increments count for each record and adds the dataSize if a record's value
   * is an instance of OmKeyInfo,RepeatedOmKeyInfo.
   * If the iterator is null, returns (0, 0, 0).
   *
   * @param iterator The iterator over the table to be iterated.
   * @return A Triple with three Long values representing the count,
   * unReplicated size and replicated size.
   * @throws IOException If an I/O error occurs during the iterator traversal.
   */
  Triple<Long, Long, Long> getTableSizeAndCount(
      TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator)
      throws IOException;

}
