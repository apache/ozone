package org.apache.hadoop.ozone.om.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestRocksDbPersistentSet {
  private ColumnFamilyHandle columnFamily;
  private RocksDB rocksDB;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    Options options = new Options().setCreateIfMissing(true);
    file = new File("./test-persistent-set");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    rocksDB = RocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();

    columnFamily = rocksDB.createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testSet"),
            new ColumnFamilyOptions()));
  }

  @AfterEach
  public void teardown() throws RocksDBException {
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
  public void testRocksDBPersistentSetForString() {
    PersistentSet<String> persistentSet = new RocksDbPersistentSet<>(
        rocksDB,
        columnFamily,
        codecRegistry,
        String.class
    );

    List<String> testList = Arrays.asList("e1", "e1", "e2", "e2", "e3");
    Set<String> testSet = new HashSet<>(testList);

    testList.forEach(persistentSet::add);

    Iterator<String> iterator = persistentSet.iterator();
    Iterator<String> setIterator = testSet.iterator();

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), setIterator.next());
    }
    assertFalse(setIterator.hasNext());
  }

  @Test
  public void testRocksDBPersistentSetForLong() {
    PersistentSet<Long> persistentSet = new RocksDbPersistentSet<>(
        rocksDB,
        columnFamily,
        codecRegistry,
        Long.class
    );

    List<Long> testList = Arrays.asList(1L, 1L, 2L, 2L, 3L, 4L, 5L, 5L);
    Set<Long> testSet = new HashSet<>(testList);

    testList.forEach(persistentSet::add);

    Iterator<Long> iterator = persistentSet.iterator();
    Iterator<Long> setIterator = testSet.iterator();

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), setIterator.next());
    }
    assertFalse(setIterator.hasNext());
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
