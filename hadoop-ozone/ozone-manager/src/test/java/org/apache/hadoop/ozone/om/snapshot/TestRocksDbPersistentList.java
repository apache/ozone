package org.apache.hadoop.ozone.om.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRocksDbPersistentList {
  private ColumnFamilyHandle columnFamily;
  private RocksDB rocksDB;
  private File file;
  private CodecRegistry codecRegistry;

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    Options options = new Options().setCreateIfMissing(true);
    file = new File("./test-persistent-list");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    rocksDB = RocksDB.open(options,
        Paths.get(file.toString(), "rocks.db").toFile().getAbsolutePath());

    codecRegistry = new CodecRegistry();
    codecRegistry.addCodec(Integer.class, new IntegerCodec());

    columnFamily = rocksDB.createColumnFamily(
        new ColumnFamilyDescriptor(
            codecRegistry.asRawData("testList"),
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

  @ValueSource(ints = {0, 3})
  @ParameterizedTest
  public void testRocksDBPersistentList(int index) {
    PersistentList<String> persistentList = new RocksDbPersistentList<>(
        rocksDB,
        columnFamily,
        codecRegistry,
        String.class
    );

    List<String> testList = Arrays.asList("e1", "e2", "e3", "e1", "e2");
    testList.forEach(persistentList::add);

    Iterator<String> iterator = index == 0 ? persistentList.iterator() :
        persistentList.iterator(index);

    while (iterator.hasNext()) {
      assertEquals(iterator.next(), testList.get(index++));
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
