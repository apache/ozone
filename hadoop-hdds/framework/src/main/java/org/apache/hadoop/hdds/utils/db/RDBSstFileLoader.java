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

import java.io.File;
import java.util.Collections;
import org.apache.hadoop.hdds.utils.db.RocksDatabase.ColumnFamily;
import org.apache.hadoop.hdds.utils.db.managed.ManagedIngestExternalFileOptions;

/**
 * Load rocksdb sst files.
 */
final class RDBSstFileLoader {
  private RDBSstFileLoader() { }

  static void load(RocksDatabase db, ColumnFamily family, File externalFile) throws RocksDatabaseException {
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
}
