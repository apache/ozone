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
import org.apache.hadoop.hdds.utils.db.managed.ManagedCheckpoint;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedFlushOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedIngestExternalFileOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteBatch;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.StringUtils.bytes2String;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions.closeDeeply;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator.managed;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedTransactionLogIterator.managed;
import static org.rocksdb.RocksDB.listColumnFamilies;

/**
 * A wrapper class for {@link org.rocksdb.RocksDB}.
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
        boolean readOnly) throws IOException {
    List<ColumnFamilyDescriptor> descriptors = null;
    ManagedRocksDB db = null;
    final Map<String, ColumnFamily> columnFamilies = new HashMap<>();
    try {
      final List<TableConfig> extra = getExtraColumnFamilies(dbFile, families);
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
    runWithTryCatch(() -> closeDeeply(d.getOptions()), new Object() {
      @Override
      public String toString() {
        return d.getClass() + ":" + bytes2String(d.getName());
      }
    });
  }

  private static void close(Map<String, ColumnFamily> columnFamilies,
      ManagedRocksDB db, List<ColumnFamilyDescriptor> descriptors,
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

    public void createCheckpoint(Path path) throws IOException {
      try {
        checkpoint.get().createCheckpoint(path.toString());
      } catch (RocksDBException e) {
        closeOnError(e);
        throw toIOException(this, "createCheckpoint " + path, e);
      }
    }

    public long getLatestSequenceNumber() {
      return RocksDatabase.this.getLatestSequenceNumber();
    }

    @Override
    public void close() throws IOException {
      checkpoint.close();
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

    public void batchDelete(ManagedWriteBatch writeBatch, byte[] key)
        throws IOException {
      try {
        writeBatch.delete(getHandle(), key);
      } catch (RocksDBException e) {
        throw toIOException(this, "batchDelete key " + bytes2String(key), e);
      }
    }

    public void batchPut(ManagedWriteBatch writeBatch, byte[] key, byte[] value)
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
  private final ManagedRocksDB db;
  private final ManagedDBOptions dbOptions;
  private final ManagedWriteOptions writeOptions;
  private final List<ColumnFamilyDescriptor> descriptors;
  private final Map<String, ColumnFamily> columnFamilies;

  private final AtomicBoolean isClosed = new AtomicBoolean();

  private RocksDatabase(File dbFile, ManagedRocksDB db,
      ManagedDBOptions dbOptions, ManagedWriteOptions writeOptions,
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
      ManagedIngestExternalFileOptions ingestOptions) throws IOException {
    try {
      db.get().ingestExternalFile(family.getHandle(), files, ingestOptions);
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
      db.get().put(family.getHandle(), writeOptions, key, value);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "put " + bytes2String(key), e);
    }
  }

  public void flush() throws IOException {
    try (ManagedFlushOptions options = new ManagedFlushOptions()) {
      options.setWaitForFlush(true);
      db.get().flush(options);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "flush", e);
    }
  }

  public void flushWal(boolean sync) throws IOException {
    try {
      db.get().flushWal(sync);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "flushWal with sync=" + sync, e);
    }
  }

  public void compactRange() throws IOException {
    try {
      db.get().compactRange();
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
   * @see org.rocksdb.RocksDB#keyMayExist(ColumnFamilyHandle, byte[], Holder)
   */
  public boolean keyMayExist(ColumnFamily family, byte[] key) {
    return db.get().keyMayExist(family.getHandle(), key, null);
  }

  /**
   * @return the null if the key definitely does not exist in the database;
   *         otherwise, return a {@link Supplier}.
   * @see org.rocksdb.RocksDB#keyMayExist(ColumnFamilyHandle, byte[], Holder)
   */
  public Supplier<byte[]> keyMayExistHolder(ColumnFamily family,
      byte[] key) {
    final Holder<byte[]> out = new Holder<>();
    return db.get().keyMayExist(family.getHandle(), key, out) ?
        out::getValue : null;
  }

  public ColumnFamily getColumnFamily(String key) {
    return columnFamilies.get(key);
  }

  public Collection<ColumnFamily> getExtraColumnFamilies() {
    return Collections.unmodifiableCollection(columnFamilies.values());
  }

  public byte[] get(ColumnFamily family, byte[] key) throws IOException {
    try {
      return db.get().get(family.getHandle(), key);
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
      return db.get().getLongProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getLongProperty " + key, e);
    }
  }

  private long getLongProperty(ColumnFamily family, String key)
      throws IOException {
    try {
      return db.get().getLongProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      final String message = "getLongProperty " + key + " from " + family;
      throw toIOException(this, message, e);
    }
  }

  public String getProperty(String key) throws IOException {
    try {
      return db.get().getProperty(key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getProperty " + key, e);
    }
  }

  public String getProperty(ColumnFamily family, String key)
      throws IOException {
    try {
      return db.get().getProperty(family.getHandle(), key);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getProperty " + key + " from " + family, e);
    }
  }

  public ManagedTransactionLogIterator getUpdatesSince(long sequenceNumber)
      throws IOException {
    try {
      return managed(db.get().getUpdatesSince(sequenceNumber));
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "getUpdatesSince " + sequenceNumber, e);
    }
  }

  public long getLatestSequenceNumber() {
    return db.get().getLatestSequenceNumber();
  }

  public ManagedRocksIterator newIterator(ColumnFamily family) {
    return managed(db.get().newIterator(family.getHandle()));
  }

  public ManagedRocksIterator newIterator(ColumnFamily family,
                                          boolean fillCache) {
    try (ManagedReadOptions readOptions = new ManagedReadOptions()) {
      readOptions.setFillCache(fillCache);
      return managed(db.get().newIterator(family.getHandle(), readOptions));
    }
  }

  public void batchWrite(ManagedWriteBatch writeBatch,
                         ManagedWriteOptions options)
      throws IOException {
    try {
      db.get().write(options, writeBatch);
    } catch (RocksDBException e) {
      closeOnError(e);
      throw toIOException(this, "batchWrite", e);
    }
  }

  public void batchWrite(ManagedWriteBatch writeBatch) throws IOException {
    batchWrite(writeBatch, writeOptions);
  }

  public void delete(ColumnFamily family, byte[] key) throws IOException {
    try {
      db.get().delete(family.getHandle(), key);
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
