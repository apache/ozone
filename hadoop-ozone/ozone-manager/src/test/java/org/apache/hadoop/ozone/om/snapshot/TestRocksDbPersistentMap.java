package org.apache.hadoop.ozone.om.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRocksDbPersistentMap {

  private ColumnFamilyHandle columnFamily;
  private RocksDB rocksDB;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    Options options = new Options().setCreateIfMissing(true);
    file = new File("./test-persistent-map");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    rocksDB = RocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();

    columnFamily = rocksDB.createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testList"),
            new ColumnFamilyOptions()));
  }

  @AfterEach
  public void clean() throws RocksDBException {
    deleteDirectory(file);

    if (columnFamily != null && rocksDB != null) {
      rocksDB.dropColumnFamily(columnFamily);
    }
    if (columnFamily != null) {
      columnFamily.close();
    }
    if (rocksDB != null) {
      rocksDB.close();
    }
  }

  @Test
  public void testRocksDBPersistentMap() {
    PersistentMap<String, String> persistentMap = new RocksDbPersistentMap<>(
        rocksDB,
        columnFamily,
        codecRegistry,
        String.class,
        String.class
    );

    List<String> keys = Arrays.asList("Key1", "Key2", "Key3", "Key1", "Key2");
    List<String> values =
        Arrays.asList("value1", "value2", "Value3", "Value1", "Value2");

    Map<String, String> expectedMap = new HashMap<>();

    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      String value = values.get(i);

      persistentMap.put(key, value);
      expectedMap.put(key, value);
    }

    for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
      assertEquals(entry.getValue(), persistentMap.get(entry.getKey()));
    }
  }

  private boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        if (!deleteDirectory(file)) {
          return false;
        }
      }
    }
    return directoryToBeDeleted.delete();
  }
}
