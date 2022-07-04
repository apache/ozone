/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.StringUtils.bytes2String;

/**
 * A wrapper class for {@link RocksDB}.
 * When there is a {@link RocksDBException} with error,
 * this class will close the underlying {@link org.rocksdb.RocksObject}s.
 */
public final class RocksDatabase {
  static final Logger LOG = LoggerFactory.getLogger(RocksDatabase.class);

  static final String ESTIMATE_NUM_KEYS = "rocksdb.estimate-num-keys";


  static IOException toIOException(Object name, String op, RocksDBException e) {
    return HddsServerUtil.toIOException(name + ": Failed to " + op, e);
  }

  /**
   * Read DB and return existing column families.
   *
   * @return a list of column families.
   */
  private static List<TableConfig> getColumnFamilies(File file)
      throws RocksDBException {
    final List<TableConfig> columnFamilies = listColumnFamiliesEmptyOptions(
        file.getAbsolutePath())
        .stream()
        .map(TableConfig::newTableConfig)
        .collect(Collectors.toList());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Found column families in DB {}: {}", file, columnFamilies);
    }
    return columnFamilies;
  }

  /**
   * Read DB column families without Options.
   * @param path
   * @return A list of column family names
   * @throws RocksDBException
   *
   * @see RocksDB#listColumnFamilies(Options, String)
   */
  public static List<byte[]> listColumnFamiliesEmptyOptions(final String path)
      throws RocksDBException {
    try (Options emptyOptions = new Options()) {
      return RocksDB.listColumnFamilies(emptyOptions, path);
    }
  }

  static RocksDatabase open(File dbFile, DBOptions dbOptions,
      WriteOptions writeOptions, Set<TableConfig> families,
      boolean readOnly) throws IOException {
    List<ColumnFamilyDescriptor> descriptors = null;
    RocksDB db = null;
    final Map<String, ColumnFamily> columnFamilies = new HashMap<>();
    try {
      // This logic has been added to support old column families that have
      // been removed, or those that may have been created in a future version.
      // TODO : Revisit this logic during upgrade implementation.
      final Stream<TableConfig> extra = getColumnFamilies(dbFile).stream()
          .filter(extraColumnFamily(families));
      descriptors = Stream.concat(families.stream(), extra)
          .map(TableConfig::getDescriptor)
          .collect(Collectors.toList());

      // open RocksDB
      final List<ColumnFamilyHandle> handles = new ArrayList<>();
      if (readOnly) {
        db = RocksDB.openReadOnly(dbOptions, dbFile.getAbsolutePath(),
            descriptors, handles);
      } else {
        db = RocksDB.open(dbOptions, dbFile.getAbsolutePath(),
            descriptors, handles);
      }
      // init a column family map.
      for (ColumnFamilyHandle h : handles) {
        final ColumnFamily f = new ColumnFamily(h);
        columnFamilies.put(f.getName(), f);
      }
      return new RocksDatabase(dbFile, db, dbOptions, writeOptions,
          descriptors, Collections.unmodifiableMap(columnFamilies));
    } catch (RocksDBException e) {
      close(columnFamilies, db, descriptors, writeOptions, dbOptions);
      throw toIOException(RocksDatabase.class, "open " + dbFile, e);
    }
  }

  private static void close(ColumnFamilyDescriptor d) {
    runWithTryCatch(() -> d.getOptions().close(), new Object() {
      @Override
      public String toString() {
        return d.getClass() + ":" + bytes2String(d.getName());
      }
    });
  }

  private static void close(Map<String, ColumnFamily> columnFamilies,
      RocksDB db, List<ColumnFamilyDescriptor> descriptors,
      WriteOptions writeOptions, DBOptions dbOptions) {
    if (columnFamilies != null) {
      for (ColumnFamily f : columnFamilies.values()) {
        runWithTryCatch(() -> f.getHandle().close(), f);
      }
    }

    if (db != null) {
      runWithTryCatch(db::close, "db");
    }

    if (descriptors != null) {
      descriptors.forEach(RocksDatabase::close);
    }

    if (writeOptions != null) {
      runWithTryCatch(writeOptions::close, "writeOptions");
    }
    if (dbOptions != null) {
      runWithTryCatch(dbOptions::close, "dbOptions");
    }
  }

  private static void runWithTryCatch(Runnable runnable, Object name) {
    try {
      runnable.run();
    } catch (Throwable t) {
      LOG.error("Failed to close " + name, t);
    }
  }

  static Predicate<TableConfig> extraColumnFamily(Set<TableConfig> families) {
    return f -> {
      if (families.contains(f)) {
        return false;
      }
      LOG.info("Found an extra column family in existing DB: {}", f);
      return true;
    };
  }

  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Represents a checkpoint of the db.
   *
   * @see Checkpoint
   */
  final class RocksCheckpoint {
    private final Checkpoint checkpoint;

    private RocksCheckpoint() {
      this.checkpoint = Checkpoint.create(db);
    }

    public void createCheckpoint(Path path) throws IOException {
      try {
        checkpoint.createCheckpoint(path.toString());
      } catch (RocksDBException e) {
        closeOnError(e);
        throw toIOException(this, "createCheckpoint " + path, e);
      }
    }

    public long getLatestSequenceNumber() {
      return RocksDatabase.this.getLatestSequenceNumber();
    }
  }

  /**
   * Represents a column family of the db.
   *
   * @see ColumnFamilyHandle
   */
  public static final class ColumnFamily {
    private final byte[] nameBytes;
    private final String name;
    private final ColumnFamilyHandle handle;

    public ColumnFamily(ColumnFamilyHandle handle) throws RocksDBException {
      this.nameBytes = handle.getName();
      this.name = bytes2String(nameBytes);
      this.handle = handle;
      LOG.debug("new ColumnFamily for {}", name);
    }

    public String getName() {
      return name;
    }

    public String getName(StringCodec codec) {
      return codec.fromPersistedFormat(nameBytes);
    }

    protected ColumnFamilyHandle getHandle() {
      return handle;
    }

    public int getID() {
      return getHandle().getID();
    }

    public void batchDelete(WriteBatch writeBatch, byte[] key)
        throws IOException {
      try {
        writeBatch.delete(getHandle(), key);
      } catch (RocksDBException e) {
        throw toIOException(this, "batchDelete key " + bytes2String(key), e);
      }
    }

    public void batchPut(WriteBatch writeBatch, byte[] key, byte[] value)
        throws IOException {
      try {
        writeBatch.put(getHandle(), key, value);
      } catch (RocksDBException e) {
        throw toIOException(this, "batchPut key " + bytes2String(key), e);
      }
    }

    @Override
    public String toString() {
      return "ColumnFamily-" + getName();
    }
  }

  private final String name;
  private final RocksDB db;
  private final DBOptions dbOptions;
  private final WriteOptions writeOptions;
  private final List<ColumnFamilyDescriptor> descriptors;
  private final Map<String, ColumnFamily> columnFamilies;

  private final AtomicBoolean isClosed = new AtomicBoolean();

  private RocksDatabase(File dbFile, RocksDB db, DBOptions dbOptions,
      WriteOptions writeOptions,
      List<ColumnFamilyDescriptor> descriptors,
      Map<String, ColumnFamily> columnFamilies) {
    this.name = getClass().getSimpleName() + "[" + dbFile + "]";
    this.db = db;
    this.dbOptions = dbOptions;
    this.writeOptions = writeOptions;
    this.descriptors = descriptors;
    this.columnFamilies = columnFamilies;
  }

  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      close(columnFamilies, db, descriptors, writeOptions, dbOptions);
    }
  }

  private void closeOnError(RocksDBException e) {
    if (shouldClose(e)) {
      close();
    }
  }

  private boolean shouldClose(RocksDBException e) {
    switch (e.getStatus().getCode()) {
    case Corruption:
    case IOError:
      return true;
    default:
      return false;
    }
  }

  public void ingestExternalFile(ColumnFamily family, List<String> files,
      IngestExternalFileOptions ingestOptions) throws IOException {
    try {
      db.ingestExternalFile(family.getHandle(), files, ingestOptions);
    } catch (RocksDBException e) {
      closeOnError(e);
      String msg = "Failed to ingest external files " +
          files.stream().collect(Collectors.joining(", ")) + " of " +
          family.getName();
      throw toIOException(this, msg, e);
    }
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws IOException {
    try {
      db.put(family.getHandle(), writeOptions, key, value);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "put " + bytes2String(key), e);
    }
  }

  public void flush() throws IOException {
    try (FlushOptions options = new FlushOptions()) {
      options.setWaitForFlush(true);
      db.flush(options);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "flush", e);
    }
  }

  public void flushWal(boolean sync) throws IOException {
    try {
      db.flushWal(sync);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "flushWal with sync=" + sync, e);
    }
  }

  public void compactRange() throws IOException {
    try {
      db.compactRange();
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "compactRange", e);
    }
  }

  RocksCheckpoint createCheckpoint() {
    return new RocksCheckpoint();
  }

  /**
   * @return false if the key definitely does not exist in the database;
   *         otherwise, return true.
   * @see RocksDB#keyMayExist(ColumnFamilyHandle, byte[], Holder)
   */
  public boolean keyMayExist(ColumnFamily family, byte[] key) {
    return db.keyMayExist(family.getHandle(), key, null);
  }

  /**
   * @return the null if the key definitely does not exist in the database;
   *         otherwise, return a {@link Supplier}.
   * @see RocksDB#keyMayExist(ColumnFamilyHandle, byte[], Holder)
   */
  public Supplier<byte[]> keyMayExistHolder(ColumnFamily family,
      byte[] key) {
    final Holder<byte[]> out = new Holder<>();
    return db.keyMayExist(family.getHandle(), key, out) ? out::getValue : null;
  }

  public ColumnFamily getColumnFamily(String key) {
    return columnFamilies.get(key);
  }

  public Collection<ColumnFamily> getColumnFamilies() {
    return Collections.unmodifiableCollection(columnFamilies.values());
  }

  public byte[] get(ColumnFamily family, byte[] key) throws IOException {
    try {
      return db.get(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "get " + bytes2String(key) + " from " + family;
      throw toIOException(this, message, e);
    }
  }

  public long estimateNumKeys() throws IOException {
    return getLongProperty(ESTIMATE_NUM_KEYS);
  }

  public long estimateNumKeys(ColumnFamily family) throws IOException {
    return getLongProperty(family, ESTIMATE_NUM_KEYS);
  }

  private long getLongProperty(String key) throws IOException {
    try {
      return db.getLongProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getLongProperty " + key, e);
    }
  }

  private long getLongProperty(ColumnFamily family, String key)
      throws IOException {
    try {
      return db.getLongProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "getLongProperty " + key + " from " + family;
      throw toIOException(this, message, e);
    }
  }

  public String getProperty(String key) throws IOException {
    try {
      return db.getProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getProperty " + key, e);
    }
  }

  public String getProperty(ColumnFamily family, String key)
      throws IOException {
    try {
      return db.getProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getProperty " + key + " from " + family, e);
    }
  }

  public TransactionLogIterator getUpdatesSince(long sequenceNumber)
      throws IOException {
    try {
      return db.getUpdatesSince(sequenceNumber);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getUpdatesSince " + sequenceNumber, e);
    }
  }

  public long getLatestSequenceNumber() {
    return db.getLatestSequenceNumber();
  }

  public RocksIterator newIterator(ColumnFamily family) {
    return db.newIterator(family.getHandle());
  }

  public RocksIterator newIterator(ColumnFamily family, boolean fillCache) {
    try (ReadOptions readOptions = new ReadOptions()) {
      readOptions.setFillCache(fillCache);
      return db.newIterator(family.getHandle(), readOptions);
    }
  }

  public void batchWrite(WriteBatch writeBatch, WriteOptions options)
      throws IOException {
    try {
      db.write(options, writeBatch);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "batchWrite", e);
    }
  }

  public void batchWrite(WriteBatch writeBatch) throws IOException {
    batchWrite(writeBatch, writeOptions);
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    try {
      db.delete(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "delete " + bytes2String(key) + " from " + family;
      throw toIOException(this, message, e);
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
