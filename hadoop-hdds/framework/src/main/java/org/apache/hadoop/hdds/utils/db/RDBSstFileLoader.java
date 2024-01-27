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

import org.apache.hadoop.hdds.utils.db.managed.ManagedIngestExternalFileOptions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;

/**
 * DumpFileLoader using rocksdb sst files.
 */
public class RDBSstFileLoader implements DumpFileLoader, Closeable {

  private final RocksDatabase db;
  private final ColumnFamily family;

  public RDBSstFileLoader(RocksDatabase db, ColumnFamily cf) {
    this.db = db;
    this.family = cf;
  }

  @Override
  public void load(File externalFile) throws IOException {
    // Ingest an empty sst file results in exception.
    if (externalFile.length() == 0) {
      return;
    }
    try (ManagedIngestExternalFileOptions ingestOptions =
             new ManagedIngestExternalFileOptions()) {
      ingestOptions.setIngestBehind(false);
      db.ingestExternalFile(family,
          Collections.singletonList(externalFile.getAbsolutePath()),
          ingestOptions);
    }
  }

  @Override
  public void close() {
  }
}
