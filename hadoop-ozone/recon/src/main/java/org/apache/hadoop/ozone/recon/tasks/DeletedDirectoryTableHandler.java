package org.apache.hadoop.ozone.recon.tasks;

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

public class DeletedDirectoryTableHandler implements  OmTableHandler {

  private ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedTableHandler.class);

  public DeletedDirectoryTableHandler(
      ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  @Override
  public void handlePutEvent(OMDBUpdateEvent<String, Object> event,
                             String tableName,
                             Collection<String> sizeRelatedTables,
                             HashMap<String, Long> objectCountMap,
                             HashMap<String, Long> unreplicatedSizeCountMap,
                             HashMap<String, Long> replicatedSizeCountMap)
      throws IOException {
    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
      Long newDeletedDirectorySize =
          fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + newDeletedDirectorySize);
    } else {
      LOG.warn("Put event does not have the Key Info for {}.",
          event.getKey());
    }
  }

  @Override
  public void handleDeleteEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Collection<String> sizeRelatedTables,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unreplicatedSizeCountMap,
                                HashMap<String, Long> replicatedSizeCountMap)
      throws IOException {
    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
      Long newDeletedDirectorySize =
          fetchSizeForDeletedDirectory(omKeyInfo.getObjectID());
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > newDeletedDirectorySize ?
              size - newDeletedDirectorySize : 0L);
    }
  }

  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Collection<String> sizeRelatedTables,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unreplicatedSizeCountMap,
                                HashMap<String, Long> replicatedSizeCountMap) {
    // The size of deleted directories cannot change hence no-op.
    return;
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

    try (TableIterator<Long, ? extends Table.KeyValue<Long, NSSummary>>
             iterator = summaryTable.iterator()) {
      // Add a for loop to iterate the entire table and transfer to a map.
      for (TableIterator<Long, ? extends Table.KeyValue<Long, NSSummary>> it =
           iterator; it.hasNext(); ) {
        Table.KeyValue<Long, NSSummary> entry = it.next();
        summaryMap.put(entry.getKey(), entry.getValue());
      }
    }

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
