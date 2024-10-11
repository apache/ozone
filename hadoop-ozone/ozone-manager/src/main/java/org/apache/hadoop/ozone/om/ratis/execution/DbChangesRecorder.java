package org.apache.hadoop.ozone.om.ratis.execution;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;

/**
 * Records db changes.
 */
public class DbChangesRecorder {
  private Map<String, Map<String, CodecBuffer>> tableRecordsMap = new HashMap<>();
  private Map<String, Pair<Long, Long>> bucketUsedQuotaMap = new HashMap<>();

  public void add(String name, String dbOpenKeyName, CodecBuffer omKeyCodecBuffer) {
    Map<String, CodecBuffer> recordMap = tableRecordsMap.computeIfAbsent(name, k -> new HashMap<>());
    recordMap.put(dbOpenKeyName, omKeyCodecBuffer);
  }
  public void add(String bucketName, long incUsedBytes, long incNamespace) {
    Pair<Long, Long> quotaPair = bucketUsedQuotaMap.get(bucketName);
    if (null == quotaPair) {
      bucketUsedQuotaMap.put(bucketName, Pair.of(incUsedBytes, incNamespace));
    } else {
      bucketUsedQuotaMap.put(bucketName, Pair.of(incUsedBytes + quotaPair.getLeft(),
          incNamespace + quotaPair.getRight()));
    }
  }
  public Map<String, Map<String, CodecBuffer>> getTableRecordsMap() {
    return tableRecordsMap;
  }
  public Map<String, Pair<Long, Long>> getBucketUsedQuotaMap() {
    return bucketUsedQuotaMap;
  }

  public void clear() {
    for (Map<String, CodecBuffer> records : tableRecordsMap.values()) {
      records.values().forEach(e -> {
        if (e != null) {
          e.release();
        }
      });
    }
    tableRecordsMap.clear();
    bucketUsedQuotaMap.clear();
  }
}
