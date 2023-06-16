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
 *
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;

/**
 * Managed BloomFilter.
 */
public class ManagedRocksDB extends ManagedObject<RocksDB> {
  public static final Class<RocksDB> ORIGINAL_CLASS = RocksDB.class;
  public static final int NOT_FOUND = RocksDB.NOT_FOUND;

  ManagedRocksDB(RocksDB original) {
    super(original);
  }

  public static ManagedRocksDB openReadOnly(
      final ManagedDBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB openReadOnly(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.openReadOnly(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB open(
      final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(options, path,
            columnFamilyDescriptors, columnFamilyHandles)
    );
  }

  public static ManagedRocksDB open(
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    return new ManagedRocksDB(
        RocksDB.open(path, columnFamilyDescriptors, columnFamilyHandles)
    );
  }
}
