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

package org.apache.hadoop.hdds.utils.db;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions.closeDeeply;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator.managed;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator.managed;
import static org.rocksdb.RocksDB.listColumnFamilies;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCheckpoint;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedFlushOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedIngestExternalFileOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.ozone.rocksdiff.RocksDiffUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.KeyMayExist;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for {@link org.rocksdb.RocksDB}.
 * When there is a {@link RocksDBException} with error,
 * this class will close the underlying {@link org.rocksdb.RocksObject}s.
 */
public final class RocksDatabase implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(RocksDatabase.class);

  public static final String ESTIMATE_NUM_KEYS = "rocksdb.estimate-num-keys";

  static {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  private static final ManagedReadOptions DEFAULT_READ_OPTION = new ManagedReadOptions();

  private final String name;
  private final Throwable creationStackTrace = new Throwable("Object creation stack trace");

  private final ManagedRocksDB db;
  private final ManagedDBOptions dbOptions;
  private final ManagedWriteOptions writeOptions;
  private final Map<String, ColumnFamilyDescriptor> descriptors;
  /** column family names -> {@link ColumnFamily}. */
  private final Map<String, ColumnFamily> columnFamilies;
  /** {@link ColumnFamilyHandle#getID()} -> column family names. */
  private final Supplier<Map<Integer, String>> columnFamilyNames;

  private final AtomicBoolean isClosed = new AtomicBoolean();
  /** Count the number of operations running concurrently. */
  private final AtomicLong counter = new AtomicLong();

  static String bytes2String(byte[] bytes) {
    return StringCodec.get().fromPersistedFormat(bytes);
  }

  static String bytes2String(ByteBuffer bytes) {
    return StringCodec.get().decode(bytes);
  }

  static RocksDatabaseException toRocksDatabaseException(Object name, String op, RocksDBException e) {
    return new RocksDatabaseException(name + ": Failed to " + op, e);
  }

  /**
   * Read DB and return existing column families.
   *
   * @return a list of column families.
   */
  private static List<TableConfig> getExtraColumnFamilies(
      File file, Set<TableConfig> families) throws RocksDBException {

    // This logic has been added to support old column families that have
    // been removed, or those that may have been created in a future version.
    // TODO : Revisit this logic during upgrade implementation.
    Set<String> existingFamilyNames = families.stream()
        .map(TableConfig::getName)
        .collect(Collectors.toSet());
    final List<TableConfig> columnFamilies = listColumnFamiliesEmptyOptions(
        file.getAbsolutePath())
        .stream()
        .map(TableConfig::toName)
        .filter(familyName -> !existingFamilyNames.contains(familyName))
        .map(familyName -> TableConfig.newTableConfig(file.toPath(), familyName))
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
   * @see org.rocksdb.RocksDB#listColumnFamilies(org.rocksdb.Options, String)
   */
  public static List<byte[]> listColumnFamiliesEmptyOptions(final String path)
      throws RocksDBException {
    try (ManagedOptions emptyOptions = new ManagedOptions()) {
      return listColumnFamilies(emptyOptions, path);
    }
  }

  static RocksDatabase open(File dbFile, ManagedDBOptions dbOptions,
        ManagedWriteOptions writeOptions, Set<TableConfig> families,
        boolean readOnly) throws RocksDatabaseException {
    List<ColumnFamilyDescriptor> descriptors = null;
    ManagedRocksDB db = null;
    final Map<String, ColumnFamily> columnFamilies = new HashMap<>();
    List<TableConfig> extra = null;
    try {
      extra = getExtraColumnFamilies(dbFile, families);
      descriptors = Stream.concat(families.stream(), extra.stream())
          .map(TableConfig::getDescriptor)
          .collect(Collectors.toList());

      // open RocksDB
      final List<ColumnFamilyHandle> handles = new ArrayList<>();
      if (readOnly) {
        db = ManagedRocksDB.openReadOnly(dbOptions, dbFile.getAbsolutePath(),
            descriptors, handles);
      } else {
        db = ManagedRocksDB.open(dbOptions, dbFile.getAbsolutePath(),
            descriptors, handles);
      }
      return new RocksDatabase(dbFile, db, dbOptions, writeOptions, descriptors, handles);
    } catch (RocksDBException e) {
      close(columnFamilies, db, descriptors, writeOptions, dbOptions);
      throw toRocksDatabaseException(RocksDatabase.class, "open " + dbFile, e);
    } finally {
      if (extra != null) {
        extra.forEach(TableConfig::close);
      }
    }
  }

  private static void close(ColumnFamilyDescriptor d) {
    ManagedColumnFamilyOptions options =
        (ManagedColumnFamilyOptions) d.getOptions();
    if (options.isReused()) {
      return;
    }
    runWithTryCatch(() -> closeDeeply(options), new Object() {
      @Override
      public String toString() {
        return d.getClass() + ":" + bytes2String(d.getName());
      }
    });
  }

  private static void close(Map<String, ColumnFamily> columnFamilies,
      ManagedRocksDB db, Collection<ColumnFamilyDescriptor> descriptors,
      ManagedWriteOptions writeOptions, ManagedDBOptions dbOptions) {
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

  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Represents a checkpoint of the db.
   *
   * @see ManagedCheckpoint
   */
  final class RocksCheckpoint implements Closeable {
    private final ManagedCheckpoint checkpoint;

    private RocksCheckpoint() {
      this.checkpoint = ManagedCheckpoint.create(db);
    }

    public void createCheckpoint(Path path) throws RocksDatabaseException {
      try (UncheckedAutoCloseable ignored = acquire()) {
        checkpoint.get().createCheckpoint(path.toString());
      } catch (RocksDBException e) {
        closeOnError(e);
        throw toRocksDatabaseException(this, "createCheckpoint " + path, e);
      }
    }

    public long getLatestSequenceNumber() throws RocksDatabaseException {
      return RocksDatabase.this.getLatestSequenceNumber();
    }

    @Override
    public void close() throws RocksDatabaseException {
      checkpoint.close();
    }
  }

  /**
   * Represents a column family of the db.
   *
   * @see ColumnFamilyHandle
   */
  public final class ColumnFamily {
    private final byte[] nameBytes;
    private final String name;
    private final ColumnFamilyHandle handle;

    private ColumnFamily(ColumnFamilyHandle handle) throws RocksDBException {
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

    @VisibleForTesting
    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public void batchDelete(ManagedWriteBatch writeBatch, byte[] key)
        throws RocksDatabaseException {
      try (UncheckedAutoCloseable ignored = acquire()) {
        writeBatch.delete(getHandle(), key);
      } catch (RocksDBException e) {
        throw toRocksDatabaseException(this, "batchDelete key " + bytes2String(key), e);
      }
    }

    public void batchDeleteRange(ManagedWriteBatch writeBatch, byte[] beginKey, byte[] endKey)
        throws RocksDatabaseException {
      try (UncheckedAutoCloseable ignored = acquire()) {
        writeBatch.deleteRange(getHandle(), beginKey, endKey);
      } catch (RocksDBException e) {
        throw toRocksDatabaseException(this, "batchDeleteRange key " + bytes2String(beginKey) + " - " +
            bytes2String(endKey), e);
      }
    }

    public void batchPut(ManagedWriteBatch writeBatch, byte[] key, byte[] value)
        throws RocksDatabaseException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("batchPut array key {}", bytes2String(key));
        LOG.debug("batchPut array value {}", bytes2String(value));
      }

      try (UncheckedAutoCloseable ignored = acquire()) {
        writeBatch.put(getHandle(), key, value);
      } catch (RocksDBException e) {
        throw toRocksDatabaseException(this, "batchPut key " + bytes2String(key), e);
      }
    }

    public void batchPut(ManagedWriteBatch writeBatch, ByteBuffer key,
        ByteBuffer value) throws RocksDatabaseException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("batchPut buffer key {}", bytes2String(key.duplicate()));
        LOG.debug("batchPut buffer value size {}", value.remaining());
      }

      try (UncheckedAutoCloseable ignored = acquire()) {
        writeBatch.put(getHandle(), key.duplicate(), value);
      } catch (RocksDBException e) {
        throw toRocksDatabaseException(this, "batchPut ByteBuffer key "
            + bytes2String(key), e);
      }
    }

    private UncheckedAutoCloseable acquire() throws RocksDatabaseException {
      if (isClosed.get()) {
        throw new RocksDatabaseException("Rocks Database is closed");
      }
      if (counter.getAndIncrement() < 0) {
        counter.getAndDecrement();
        throw new RocksDatabaseException("Rocks Database is closed");
      }
      return counter::getAndDecrement;
    }

    @Override
    public String toString() {
      return "ColumnFamily-" + getName();
    }
  }

  private RocksDatabase(File dbFile, ManagedRocksDB db,
      ManagedDBOptions dbOptions, ManagedWriteOptions writeOptions,
      List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) throws RocksDBException {
    this.name = getClass().getSimpleName() + "[" + dbFile + "]";
    this.db = db;
    this.dbOptions = dbOptions;
    this.writeOptions = writeOptions;
    this.descriptors = descriptors.stream().collect(Collectors.toMap(d -> bytes2String(d.getName()), d -> d,
        (d1, d2) -> {
        throw new IllegalStateException("Duplicate key " + bytes2String(d1.getName()));
      }, ConcurrentHashMap::new));
    this.columnFamilies = toColumnFamilyMap(handles);
    this.columnFamilyNames = MemoizedSupplier.valueOf(() -> toColumnFamilyNameMap(columnFamilies.values()));
  }

  private Map<String, ColumnFamily> toColumnFamilyMap(List<ColumnFamilyHandle> handles) throws RocksDBException {
    final Map<String, ColumnFamily> map = new ConcurrentHashMap<>(handles.size());
    for (ColumnFamilyHandle h : handles) {
      final ColumnFamily f = new ColumnFamily(h);
      map.put(f.getName(), f);
    }
    return map;
  }

  private static Map<Integer, String> toColumnFamilyNameMap(Collection<ColumnFamily> families) {
    return Collections.unmodifiableMap(families.stream()
        .collect(Collectors.toMap(f -> f.getHandle().getID(), ColumnFamily::getName)));
  }

  Map<Integer, String> getColumnFamilyNames() {
    return columnFamilyNames.get();
  }

  @Override
  public void close() {
    close(true);
  }

  private void close(boolean isSync) {
    if (isClosed.compareAndSet(false, true)) {
      // Wait for all background work to be cancelled first. e.g. RDB compaction
      db.get().cancelAllBackgroundWork(true);

      // Then close all attached listeners
      dbOptions.listeners().forEach(listener -> listener.close());

      if (isSync) {
        waitAndClose();
        return;
      }
      // async trigger the close event
      new Thread(() -> waitAndClose(), "DBCloser-" + name).start();
    }
  }

  private void waitAndClose() {
    // wait till all access to rocks db is process to avoid crash while close
    while (!counter.compareAndSet(0, Long.MIN_VALUE)) {
      try {
        Thread.currentThread().sleep(1);
      } catch (InterruptedException e) {
        close(columnFamilies, db, descriptors.values(), writeOptions, dbOptions);
        Thread.currentThread().interrupt();
        return;
      }
    }

    // close when counter is 0, no more operation
    close(columnFamilies, db, descriptors.values(), writeOptions, dbOptions);
  }

  private void closeOnError(RocksDBException e) {
    if (shouldClose(e)) {
      close(false);
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

  private UncheckedAutoCloseable acquire() throws RocksDatabaseException {
    if (isClosed()) {
      throw new RocksDatabaseException("Rocks Database is closed");
    }
    if (counter.getAndIncrement() < 0) {
      counter.getAndDecrement();
      throw new RocksDatabaseException("Rocks Database is closed");
    }
    return counter::getAndDecrement;
  }

  public void ingestExternalFile(ColumnFamily family, List<String> files,
      ManagedIngestExternalFileOptions ingestOptions) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().ingestExternalFile(family.getHandle(), files, ingestOptions);
    } catch (RocksDBException e) {
      closeOnError(e);
      String msg = "Failed to ingest external files " +
          files.stream().collect(Collectors.joining(", ")) + " of " +
          family.getName();
      throw toRocksDatabaseException(this, msg, e);
    }
  }

  public void put(ColumnFamily family, byte[] key, byte[] value)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().put(family.getHandle(), writeOptions, key, value);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "put " + bytes2String(key), e);
    }
  }

  public void put(ColumnFamily family, ByteBuffer key, ByteBuffer value)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().put(family.getHandle(), writeOptions, key, value);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "put " + bytes2String(key), e);
    }
  }

  public void flush() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire();
        ManagedFlushOptions options = new ManagedFlushOptions()) {
      options.setWaitForFlush(true);
      db.get().flush(options);
      for (RocksDatabase.ColumnFamily columnFamily : getExtraColumnFamilies()) {
        db.get().flush(options, columnFamily.handle);
      }
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "flush", e);
    }
  }

  /**
   * @param cfName columnFamily on which flush will run.
   */
  public void flush(String cfName) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      ColumnFamilyHandle handle = getColumnFamilyHandle(cfName);
      try (ManagedFlushOptions options = new ManagedFlushOptions()) {
        options.setWaitForFlush(true);
        if (handle != null) {
          db.get().flush(options, handle);
        } else {
          LOG.error("Provided column family doesn't exist."
              + " Calling flush on null columnFamily");
          flush();
        }
      } catch (RocksDBException e) {
        closeOnError(e);
        throw toRocksDatabaseException(this, "flush", e);
      }
    }
  }

  public void flushWal(boolean sync) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().flushWal(sync);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "flushWal with sync=" + sync, e);
    }
  }

  public void compactRange() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().compactRange();
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "compactRange", e);
    }
  }

  public void compactRangeDefault(final ManagedCompactRangeOptions options)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().compactRange(null, null, null, options);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "compactRange", e);
    }
  }

  public void compactDB(ManagedCompactRangeOptions options) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      compactRangeDefault(options);
      for (RocksDatabase.ColumnFamily columnFamily
          : getExtraColumnFamilies()) {
        compactRange(columnFamily, null, null, options);
      }
    }
  }

  public int getLiveFilesMetaDataSize() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLiveFilesMetaData().size();
    }
  }

  /**
   * @param cfName columnFamily on which compaction will run.
   */
  public void compactRange(String cfName) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      ColumnFamilyHandle handle = getColumnFamilyHandle(cfName);
      try {
        if (handle != null) {
          db.get().compactRange(handle);
        } else {
          LOG.error("Provided column family doesn't exist."
              + " Calling compactRange on null columnFamily");
          db.get().compactRange();
        }
      } catch (RocksDBException e) {
        closeOnError(e);
        throw toRocksDatabaseException(this, "compactRange", e);
      }
    }
  }

  private ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) {
    final ColumnFamily columnFamily = getColumnFamily(columnFamilyName);
    return columnFamily != null ? columnFamily.getHandle() : null;
  }

  public void compactRange(ColumnFamily family, final byte[] begin,
      final byte[] end, final ManagedCompactRangeOptions options)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().compactRange(family.getHandle(), begin, end, options);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "compactRange", e);
    }
  }

  public List<LiveFileMetaData> getLiveFilesMetaData() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLiveFilesMetaData();
    }
  }

  RocksCheckpoint createCheckpoint() {
    return new RocksCheckpoint();
  }

  /**
   * - When the key definitely does not exist in the database,
   *   this method returns null.
   * - When the key is found in memory,
   *   this method returns a supplier
   *   and {@link Supplier#get()}} returns the value.
   * - When this method returns a supplier
   *   but {@link Supplier#get()} returns null,
   *   the key may or may not exist in the database.
   *
   * @return the null if the key definitely does not exist in the database;
   *         otherwise, return a {@link Supplier}.
   * @see org.rocksdb.RocksDB#keyMayExist(ColumnFamilyHandle, byte[], Holder)
   */
  Supplier<byte[]> keyMayExist(ColumnFamily family, byte[] key)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      final Holder<byte[]> out = new Holder<>();
      return db.get().keyMayExist(family.getHandle(), key, out) ?
          out::getValue : null;
    }
  }

  Supplier<Integer> keyMayExist(ColumnFamily family,
      ByteBuffer key, ByteBuffer out) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      final KeyMayExist result = db.get().keyMayExist(
          family.getHandle(), key, out);
      switch (result.exists) {
      case kNotExist: return null;
      case kExistsWithValue: return () -> result.valueLength;
      case kExistsWithoutValue: return () -> null;
      default:
        throw new IllegalStateException(
            "Unexpected KeyMayExistEnum case " + result.exists);
      }
    }
  }

  public ColumnFamily getColumnFamily(String key) {
    return columnFamilies.get(key);
  }

  public Collection<ColumnFamily> getExtraColumnFamilies() {
    return Collections.unmodifiableCollection(columnFamilies.values());
  }

  public void dropColumnFamily(String tableName) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      ColumnFamily columnFamily = columnFamilies.get(tableName);
      if (columnFamily != null) {
        try {
          getManagedRocksDb().get().dropColumnFamily(columnFamily.getHandle());
          ColumnFamilyDescriptor descriptor = descriptors.get(tableName);
          columnFamily.getHandle().close();
          if (descriptor != null) {
            RocksDatabase.close(descriptor);
          }
          columnFamilies.remove(tableName);
          descriptors.remove(tableName);
        } catch (RocksDBException e) {
          closeOnError(e);
          throw toRocksDatabaseException(this, "DropColumnFamily " + tableName, e);
        }
      }
    }
  }

  byte[] get(ColumnFamily family, byte[] key) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().get(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "get " + bytes2String(key) + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  /**
   * Get the value mapped to the given key.
   *
   * @param family the table to get from.
   * @param key the buffer containing the key.
   * @param outValue the buffer to store the output value.
   *                 When the buffer size is smaller than the size of the value,
   *                 partial result will be written.
   * @return null if the key is not found;
   *         otherwise, return the size (possibly 0) of the value.
   * @throws RocksDatabaseException if the db is closed or the db throws an exception.
   * @see org.rocksdb.RocksDB#get(ColumnFamilyHandle, org.rocksdb.ReadOptions,
   *                              ByteBuffer, ByteBuffer)
   */
  Integer get(ColumnFamily family, ByteBuffer key, ByteBuffer outValue)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      final int size = db.get().get(family.getHandle(),
          DEFAULT_READ_OPTION, key, outValue);
      LOG.trace("get: size={}, remaining={}",
          size, outValue.asReadOnlyBuffer().remaining());
      return size == ManagedRocksDB.NOT_FOUND ? null : size;
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "get " + bytes2String(key) + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  public long estimateNumKeys() throws RocksDatabaseException {
    return getLongProperty(ESTIMATE_NUM_KEYS);
  }

  public long estimateNumKeys(ColumnFamily family) throws RocksDatabaseException {
    return getLongProperty(family, ESTIMATE_NUM_KEYS);
  }

  private long getLongProperty(String key) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLongProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "getLongProperty " + key, e);
    }
  }

  private long getLongProperty(ColumnFamily family, String key)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLongProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "getLongProperty " + key + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  public String getProperty(String key) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "getProperty " + key, e);
    }
  }

  public String getProperty(ColumnFamily family, String key)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "getProperty " + key + " from " + family, e);
    }
  }

  public ManagedTransactionLogIterator getUpdatesSince(long sequenceNumber)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return managed(db.get().getUpdatesSince(sequenceNumber));
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "getUpdatesSince " + sequenceNumber, e);
    }
  }

  public long getLatestSequenceNumber() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLatestSequenceNumber();
    }
  }

  public ManagedRocksIterator newIterator(ColumnFamily family)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return managed(db.get().newIterator(family.getHandle()));
    }
  }

  public ManagedRocksIterator newIterator(ColumnFamily family,
      boolean fillCache) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire();
         ManagedReadOptions readOptions = new ManagedReadOptions()) {
      readOptions.setFillCache(fillCache);
      return managed(db.get().newIterator(family.getHandle(), readOptions));
    }
  }

  public void batchWrite(ManagedWriteBatch writeBatch,
                         ManagedWriteOptions options)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().write(options, writeBatch);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toRocksDatabaseException(this, "batchWrite", e);
    }
  }

  public void batchWrite(ManagedWriteBatch writeBatch) throws RocksDatabaseException {
    batchWrite(writeBatch, writeOptions);
  }

  public void delete(ColumnFamily family, byte[] key) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().delete(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "delete " + bytes2String(key) + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  public void delete(ColumnFamily family, ByteBuffer key) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().delete(family.getHandle(), writeOptions, key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "delete " + bytes2String(key) + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  public void deleteRange(ColumnFamily family, byte[] beginKey, byte[] endKey)
      throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      db.get().deleteRange(family.getHandle(), beginKey, endKey);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "delete range " + bytes2String(beginKey) +
          " to " + bytes2String(endKey) + " from " + family;
      throw toRocksDatabaseException(this, message, e);
    }
  }

  @Override
  public String toString() {
    return name;
  }

  @VisibleForTesting
  public List<LiveFileMetaData> getSstFileList() throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      return db.get().getLiveFilesMetaData();
    }
  }

  /**
   * return the max compaction level of sst files in the db.
   * @return level
   */
  private int getLastLevel() throws RocksDatabaseException {
    return getSstFileList().stream()
        .max(Comparator.comparing(LiveFileMetaData::level)).get().level();
  }

  /**
   * Deletes sst files which do not correspond to prefix
   * for given table.
   * @param prefixInfo a map of TableName to prefixUsed.
   */
  public void deleteFilesNotMatchingPrefix(TablePrefixInfo prefixInfo) throws RocksDatabaseException {
    try (UncheckedAutoCloseable ignored = acquire()) {
      for (LiveFileMetaData liveFileMetaData : getSstFileList()) {
        String sstFileColumnFamily = StringUtils.bytes2String(liveFileMetaData.columnFamilyName());
        int lastLevel = getLastLevel();

        // RocksDB #deleteFile API allows only to delete the last level of
        // SST Files. Any level < last level won't get deleted and
        // only last file of level 0 can be deleted
        // and will throw warning in the rocksdb manifest.
        // Instead, perform the level check here
        // itself to avoid failed delete attempts for lower level files.
        if (liveFileMetaData.level() != lastLevel || lastLevel == 0) {
          continue;
        }

        String prefixForColumnFamily = prefixInfo.getTablePrefix(sstFileColumnFamily);
        String firstDbKey = StringUtils.bytes2String(liveFileMetaData.smallestKey());
        String lastDbKey = StringUtils.bytes2String(liveFileMetaData.largestKey());
        boolean isKeyWithPrefixPresent = RocksDiffUtils.isKeyWithPrefixPresent(
            prefixForColumnFamily, firstDbKey, lastDbKey);
        if (!isKeyWithPrefixPresent) {
          LOG.info("Deleting sst file: {} with start key: {} and end key: {} "
                  + "corresponding to column family {} from db: {}. "
                  + "Prefix for the column family: {}.",
              liveFileMetaData.fileName(),
              firstDbKey, lastDbKey,
              StringUtils.bytes2String(liveFileMetaData.columnFamilyName()),
              db.get().getName(),
              prefixForColumnFamily);
          db.deleteFile(liveFileMetaData);
        }
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    if (!isClosed()) {
      LOG.warn("RocksDatabase {} is not closed properly.", name, creationStackTrace);
    }
    super.finalize();
  }

  public ManagedRocksDB getManagedRocksDb() {
    return db;
  }
}
