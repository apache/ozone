package org.apache.hadoop.ozone.recon.tasks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public class DeletedTableHandler implements OmTableHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeletedTableHandler.class);

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
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count + repeatedOmKeyInfo.getOmKeyInfoList().size());
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size + result.getLeft());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size + result.getRight());
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
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey, (k, count) ->
          count > 0 ? count - repeatedOmKeyInfo.getOmKeyInfoList().size() : 0L);
      Pair<Long, Long> result = repeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size > result.getLeft() ? size - result.getLeft() : 0L);
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size > result.getRight() ? size - result.getRight() :
              0L);
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

    String countKey = getTableCountKeyFromTable(tableName);
    String unReplicatedSizeKey = getUnReplicatedSizeKeyFromTable(tableName);
    String replicatedSizeKey = getReplicatedSizeKeyFromTable(tableName);

    if (event.getValue() != null) {
      if (event.getOldValue() == null) {
        LOG.warn("Update event does not have the old Key Info for {}.",
            event.getKey());
        return;
      }
      RepeatedOmKeyInfo oldRepeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getOldValue();
      RepeatedOmKeyInfo newRepeatedOmKeyInfo =
          (RepeatedOmKeyInfo) event.getValue();
      objectCountMap.computeIfPresent(countKey,
          (k, count) -> count > 0 ?
              count - oldRepeatedOmKeyInfo.getOmKeyInfoList().size() +
                  newRepeatedOmKeyInfo.getOmKeyInfoList().size() : 0L);
      Pair<Long, Long> oldSize = oldRepeatedOmKeyInfo.getTotalSize();
      Pair<Long, Long> newSize = newRepeatedOmKeyInfo.getTotalSize();
      unreplicatedSizeCountMap.computeIfPresent(unReplicatedSizeKey,
          (k, size) -> size - oldSize.getLeft() + newSize.getLeft());
      replicatedSizeCountMap.computeIfPresent(replicatedSizeKey,
          (k, size) -> size - oldSize.getRight() + newSize.getRight());

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
