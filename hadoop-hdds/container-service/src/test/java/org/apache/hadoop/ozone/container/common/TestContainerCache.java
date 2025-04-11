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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ContainerCacheMetrics;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.junit.jupiter.api.Test;

/**
 * Test ContainerCache with evictions.
 */
public class TestContainerCache {
  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  private void createContainerDB(OzoneConfiguration conf, File dbFile)
      throws Exception {
    DatanodeStore store = new DatanodeStoreSchemaTwoImpl(
        conf, dbFile.getAbsolutePath(), false);

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.stop();
  }

  @Test
  public void testContainerCacheEviction() throws Exception {
    File root = new File(testRoot);
    assertTrue(root.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);

    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    assertEquals(0, cache.size());
    File containerDir1 = new File(root, "cont1");
    File containerDir2 = new File(root, "cont2");
    File containerDir3 = new File(root, "cont3");
    File containerDir4 = new File(root, "cont4");


    createContainerDB(conf, containerDir1);
    createContainerDB(conf, containerDir2);
    createContainerDB(conf, containerDir3);
    createContainerDB(conf, containerDir4);

    ContainerCacheMetrics metrics = cache.getMetrics();
    long numDbGetCount = metrics.getNumDbGetOps();
    long numCacheMisses = metrics.getNumCacheMisses();
    // Get 2 references out of the same db and verify the objects are same.
    ReferenceCountedDB db1 = cache.getDB(1, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertEquals(1, db1.getReferenceCount());
    assertEquals(numDbGetCount + 1, metrics.getNumDbGetOps());
    ReferenceCountedDB db2 = cache.getDB(1, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertEquals(2, db2.getReferenceCount());
    assertEquals(numCacheMisses + 1, metrics.getNumCacheMisses());
    assertEquals(2, db1.getReferenceCount());
    assertEquals(db1, db2);
    assertEquals(numDbGetCount + 2, metrics.getNumDbGetOps());
    assertEquals(numCacheMisses + 1, metrics.getNumCacheMisses());

    // add one more references to ContainerCache.
    ReferenceCountedDB db3 = cache.getDB(2, "RocksDB",
        containerDir2.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertEquals(1, db3.getReferenceCount());

    // and close the reference
    db3.close();
    assertEquals(0, db3.getReferenceCount());

    // add one more reference to ContainerCache and verify that it will not
    // evict the least recent entry as it has reference.
    ReferenceCountedDB db4 = cache.getDB(3, "RocksDB",
        containerDir3.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertEquals(1, db4.getReferenceCount());

    assertEquals(2, cache.size());
    assertNotNull(cache.get(containerDir1.getPath()));
    assertNull(cache.get(containerDir2.getPath()));

    // Now close both the references for container1
    db1.close();
    db2.close();
    assertEquals(0, db1.getReferenceCount());
    assertEquals(0, db2.getReferenceCount());

    // The reference count for container1 is 0 but it is not evicted.
    ReferenceCountedDB db5 = cache.getDB(1, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertThrows(IllegalArgumentException.class, () -> {
      assertEquals(1, db5.getReferenceCount());
      assertEquals(db1, db5);
      db5.close();
      db4.close();
      db5.close();
    });

    FileUtils.deleteDirectory(root);
  }

  @Test
  void testConcurrentDBGet() throws Exception {
    File root = new File(testRoot);
    assertTrue(root.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    assertEquals(0, cache.size());
    File containerDir = new File(root, "cont1");
    createContainerDB(conf, containerDir);
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Runnable task = () -> {
      try {
        ReferenceCountedDB db1 = cache.getDB(1, "RocksDB",
            containerDir.getPath(),
            VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
        assertNotNull(db1);
      } catch (IOException e) {
        fail("Should get the DB instance");
      }
    };
    List<Future> futureList = new ArrayList<>();
    futureList.add(executorService.submit(task));
    futureList.add(executorService.submit(task));
    for (Future future: futureList) {
      future.get();
    }

    ReferenceCountedDB db = cache.getDB(1, "RocksDB",
        containerDir.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    db.close();
    db.close();
    db.close();
    assertEquals(1, cache.size());
    db.cleanup();
    FileUtils.deleteDirectory(root);
  }

  @Test
  public void testUnderlyingDBzIsClosed() throws Exception {
    File root = new File(testRoot);
    assertTrue(root.mkdirs());

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);

    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    assertEquals(0, cache.size());
    File containerDir1 = new File(root, "cont100");

    createContainerDB(conf, containerDir1);
    ReferenceCountedDB db1 = cache.getDB(100, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    ReferenceCountedDB db2 = cache.getDB(100, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertEquals(db1, db2);
    db1.getStore().getStore().close();
    ReferenceCountedDB db3 = cache.getDB(100, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    ReferenceCountedDB db4 = cache.getDB(100, "RocksDB",
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    assertNotEquals(db3, db2);
    assertEquals(db4, db3);
    db1.close();
    db2.close();
    db3.close();
    db4.close();
    cache.clear();
    FileUtils.deleteDirectory(root);
  }
}
