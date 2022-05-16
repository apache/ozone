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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.toIOException;

/**
 * DumpFileLoader using rocksdb sst files.
 */
public class RDBSstFileLoader implements DumpFileLoader {

  private final RocksDB db;
  private final ColumnFamilyHandle handle;
  private final IngestExternalFileOptions ingestOptions;


  public RDBSstFileLoader(RocksDB db, ColumnFamilyHandle handle) {
    this.db = db;
    this.handle = handle;
    this.ingestOptions = new IngestExternalFileOptions()
        .setIngestBehind(false);
  }

  @Override
  public void load(File externalFile) throws IOException {
    // Ingest an empty sst file results in exception.
    if (externalFile.length() == 0) {
      return;
    }

    try {
      db.ingestExternalFile(handle,
          Collections.singletonList(externalFile.getAbsolutePath()),
          ingestOptions);
    } catch (RocksDBException e) {
      throw toIOException("Failed to ingest external file "
          + externalFile.getAbsolutePath() + ", ingestBehind:"
          + ingestOptions.ingestBehind(), e);
    }
  }
}