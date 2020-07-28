package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SchemaOneDeletedBlocksTable extends TypedTable<String, NoData> {

  private void checkKey(String key) {

  }

  @Override
  public void put(String key, NoData value) throws IOException {
    checkKey(key);
    super.put(key, value);
  }

  @Override
  public void putWithBatch(BatchOperation batch, String key, NoData value)
          throws IOException {
    checkKey(key);
    super.putWithBatch(batch, key, value);
  }

  @Override
  public void addCacheEntry(CacheKey<String> cacheKey,
                            CacheValue<NoData> cacheValue) {
    checkKey(cacheKey.getCacheKey());
    // This will override the entry if there is already entry for this key.
    super.addCacheEntry(cacheKey, cacheValue);
  }

  @Override
  public List<TypedKeyValue> getRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    List<MetadataKeyFilters.MetadataKeyFilter> newFilters =
            Arrays.asList(filters);
    newFilters.add(MetadataKeyFilters.getDeletingKeyFilter());

    return super.getRangeKVs(startKey, count,
            newFilters.toArray();
  }

  @Override
  public List<TypedKeyValue> getSequentialRangeKVs(
          String startKey, int count,
          MetadataKeyFilters.MetadataKeyFilter... filters)
          throws IOException, IllegalArgumentException {

    return super.getSequentialRangeKVs();
  }

  // Handle varargs for sequential range queries.
  // Maybe check if deleted prefix filter was already added.
  private MetadataKeyFilters.MetadataKeyFilter[] addFilter() {

  }
}
