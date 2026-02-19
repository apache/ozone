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

package org.apache.hadoop.ozone.container.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.RawDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test DatanodeStoreCache.
 */
public class TestDatanodeStoreCache {
  @TempDir
  private Path folder;

  private OzoneConfiguration conf = new OzoneConfiguration();

  @AfterEach
  void cleanupCache() {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    cache.resetStoreFactory();
    cache.shutdownCache();
  }

  @Test
  void testBasicOperations() throws IOException {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    String dbPath1 = Files.createDirectory(folder.resolve("basic1"))
        .toFile().toString();
    String dbPath2 = Files.createDirectory(folder.resolve("basic2"))
        .toFile().toString();
    DatanodeStore store1 = new DatanodeStoreSchemaThreeImpl(conf, dbPath1,
        false);
    DatanodeStore store2 = new DatanodeStoreSchemaThreeImpl(conf, dbPath2,
        false);

    // test normal add
    cache.addDB(dbPath1, new RawDB(store1, dbPath1));
    cache.addDB(dbPath2, new RawDB(store2, dbPath2));
    assertEquals(2, cache.size());

    // test duplicate add
    cache.addDB(dbPath1, new RawDB(store1, dbPath1));
    assertEquals(2, cache.size());

    // test get, test reference the same object using ==
    assertSame(store1, cache.getDB(dbPath1, conf).getStore());

    // test remove
    cache.removeDB(dbPath1);
    assertEquals(1, cache.size());

    // test remove non-exist
    cache.removeDB(dbPath1);

    // test shutdown
    cache.shutdownCache();
    assertEquals(0, cache.size());
  }

  @Test
  void testClosedStoreIsReopened() throws IOException {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    cache.shutdownCache();
    String dbPath = Files.createDirectory(folder.resolve("reopen"))
        .toFile().toString();
    DatanodeStore store = new DatanodeStoreSchemaThreeImpl(conf, dbPath, false);
    cache.addDB(dbPath, new RawDB(store, dbPath));

    RawDB db1 = cache.getDB(dbPath, conf);
    db1.getStore().stop();

    LogCapturer cacheLogs = LogCapturer.captureLogs(DatanodeStoreCache.class);
    try {
      RawDB db2 = cache.getDB(dbPath, conf);
      assertNotSame(db1, db2);
      assertNotSame(db1.getStore(), db2.getStore());
      assertSame(db2, cache.getDB(dbPath, conf));
      assertTrue(cacheLogs.getOutput()
          .contains("Removed closed db " + dbPath + " from cache"));
    } finally {
      cacheLogs.stopCapturing();
    }

    cache.shutdownCache();
  }

  @Test
  void testNoSpaceOpenFallsBackToReadOnly() throws IOException {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    String dbPath = Files.createDirectory(folder.resolve("readonly-fallback"))
        .toFile().toString();

    DatanodeStore preCreatedStore = new DatanodeStoreSchemaThreeImpl(
        conf, dbPath, false);
    preCreatedStore.stop();

    final boolean[] readOnlyAttempted = {false};
    cache.setStoreFactory((config, path, readOnly) -> {
      if (!readOnly) {
        throw new IOException("RW open failed",
            new RocksDatabaseException("IOError: No space left on device"));
      }
      readOnlyAttempted[0] = true;
      return new DatanodeStoreSchemaThreeImpl(config, path, true);
    });

    LogCapturer cacheLogs = LogCapturer.captureLogs(DatanodeStoreCache.class);
    try {
      RawDB db = cache.getDB(dbPath, conf);
      assertTrue(readOnlyAttempted[0]);
      assertThrows(IOException.class, () -> db.getStore().getMetadataTable().put("key", 1L));
      assertFalse(db.getStore().isClosed());

      String logOutput = cacheLogs.getOutput();
      assertTrue(logOutput.contains("Retrying read-only."));
      assertTrue(logOutput.contains("Opened db " + dbPath + " in read-only "
          + "mode after read-write open failed due to no space"));
    } finally {
      cacheLogs.stopCapturing();
    }
  }

  @Test
  void testNonNoSpaceOpenFailureDoesNotFallbackToReadOnly() throws IOException {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    String dbPath = Files.createDirectory(folder.resolve("non-nospace"))
        .toFile().toString();

    final boolean[] readOnlyAttempted = {false};
    cache.setStoreFactory((config, path, readOnly) -> {
      if (readOnly) {
        readOnlyAttempted[0] = true;
      }
      throw new IOException("RW open failed due to permission denied");
    });

    assertThrows(IOException.class, () -> cache.getDB(dbPath, conf));
    assertFalse(readOnlyAttempted[0]);
  }
}
