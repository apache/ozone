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

package org.apache.hadoop.ozone.container.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests deletion-triggered compaction in datanode container DB profiles. */
public class TestDatanodeDBProfile {

  @ParameterizedTest
  @CsvSource({"SSD, 2", "SSD, 3", "DISK, 2", "DISK, 3"})
  void testCompactionOnDeletion(DBProfile profile, int schema, @TempDir Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setEnum(HddsConfigKeys.HDDS_DB_PROFILE, profile);
    conf.setDouble("hadoop.hdds.db.rocksdb.compaction.deletion.ratio", 0.5);
    DatanodeStore datanodeStore = schema == 2
        ? new DatanodeStoreSchemaTwoImpl(conf, tempDir.resolve("container.db").toString(), false)
        : new DatanodeStoreSchemaThreeImpl(conf, tempDir.resolve("container.db").toString(), false);
    try {
      RDBStore store = (RDBStore) datanodeStore.getStore();
      Table<String, Long> table = datanodeStore.getMetadataTable();
      for (int i = 0; i < 1000; i++) {
        table.put(String.format("%020d", i), (long) i);
      }
      store.flushDB();
      for (int i = 0; i < 750; i++) {
        table.delete(String.format("%020d", i));
      }
      store.flushDB();
      GenericTestUtils.waitFor(() -> {
        try {
          return store.getDb().getManagedRocksDb().get()
              .getPropertiesOfAllTables(store.getDb().getColumnFamily(table.getName()).getHandle()).values().stream()
              .mapToLong(properties -> properties.getNumDeletions()).sum() == 0;
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, 100, 10000);
      for (int i = 0; i < 750; i++) {
        assertNull(table.get(String.format("%020d", i)));
      }
      for (int i = 750; i < 1000; i++) {
        assertEquals(Long.valueOf(i), table.get(String.format("%020d", i)));
      }
      int count = 0;
      Table<byte[], byte[]> rawTable = store.getTable(table.getName());
      try (Table.KeyValueIterator<byte[], byte[]> iterator = rawTable.iterator()) {
        while (iterator.hasNext()) {
          iterator.next();
          count++;
        }
      }
      assertEquals(250, count);
    } finally {
      // Container DBs share profile options; stop the store without closing the shared options.
      datanodeStore.stop();
    }
  }
}
