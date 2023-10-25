package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;


public class OpenKeyTableHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpenKeyTableHandler.class);

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
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) -> count + 1L);
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + omKeyInfo.getDataSize());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size + omKeyInfo.getReplicatedSize());
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
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      OmKeyInfo omKeyInfo = (OmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count > 0 ? count - 1L : 0L);
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > omKeyInfo.getDataSize() ?
              size - omKeyInfo.getDataSize() : 0L);
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size > omKeyInfo.getReplicatedSize() ?
              size - omKeyInfo.getReplicatedSize() : 0L);
    } else {
      LOG.warn("Delete event does not have the Key Info for {}.",
          event.getKey());
    }
  }


  @Override
  public void handleUpdateEvent(OMDBUpdateEvent<String, Object> event,
                                String tableName,
                                Collection<String> sizeRelatedTables,
                                HashMap<String, Long> objectCountMap,
                                HashMap<String, Long> unreplicatedSizeCountMap,
                                HashMap<String, Long> replicatedSizeCountMap) {
    if (event.getValue() != null) {
      if (event.getOldValue() == null) {
        LOG.warn("Update event does not have the old Key Info for {}.",
            event.getKey());
        return;
      }
      String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
      String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

      // In Update event the count for the open table will not change. So we don't
      // need to update the count.
      OmKeyInfo oldKeyInfo = (OmKeyInfo) event.getOldValue();
      OmKeyInfo newKeyInfo = (OmKeyInfo) event.getValue();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size - oldKeyInfo.getDataSize() +
              newKeyInfo.getDataSize());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size - oldKeyInfo.getReplicatedSize() +
              newKeyInfo.getReplicatedSize());
    } else {
      LOG.warn("Update event does not have the Key Info for {}.",
          event.getKey());
    }
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