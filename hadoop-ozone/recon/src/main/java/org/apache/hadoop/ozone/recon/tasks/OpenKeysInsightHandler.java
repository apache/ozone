package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.io.IOException;
import java.util.HashMap;

public class OpenKeysInsightHandler {

  public void reprocess(
      HashMap<String, Long> objectCountMap,
      HashMap<String, Long> unReplicatedSizeCountMap,
      HashMap<String, Long> replicatedSizeCountMap,
      TableIterator<String, ? extends Table.KeyValue<String, ?>> iterator,
      String tableName)
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
    objectCountMap.put(getTableCountKeyFromTable(tableName), count);
    unReplicatedSizeCountMap.put(
        getUnReplicatedSizeKeyFromTable(tableName), unReplicatedSize);
    replicatedSizeCountMap.put(getReplicatedSizeKeyFromTable(tableName),
        replicatedSize);
  }

  public void process() {

  }

  public static String getTableCountKeyFromTable(String tableName) {
    return tableName + "TableCount";
  }

  public static String getReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "ReplicatedDataSize";
  }

  public static String getUnReplicatedSizeKeyFromTable(String tableName) {
    return tableName + "UnReplicatedDataSize";
  }

}
