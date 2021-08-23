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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.ContainerCacheMetrics;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaTwoImpl;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Test ContainerCache with evictions.
 */
public class TestContainerCache {
  private static String testRoot = new FileSystemTestHelper().getTestRootDir();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private ReferenceCountedDB createContainerDB(OzoneConfiguration conf,
      File dbFile) throws Exception {
    DatanodeStore store = new DatanodeStoreSchemaTwoImpl(
            conf, 1, dbFile.getAbsolutePath(), false);

    // we close since the SCM pre-creates containers.
    // we will open and put Db handle into a cache when keys are being created
    // in a container.

    store.stop();
    ReferenceCountedDB db =
        new ReferenceCountedDB(store, dbFile.getAbsolutePath());
    return db;
  }

  @Test
  public void testContainerCacheEviction() throws Exception {
    File root = new File(testRoot);
    root.mkdirs();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);

    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    Assert.assertEquals(0, cache.size());
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
    ReferenceCountedDB db1 = cache.getDB(1, State.CLOSED,
            containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    Assert.assertEquals(1, db1.getReferenceCount());
    Assert.assertEquals(numDbGetCount + 1, metrics.getNumDbGetOps());
    ReferenceCountedDB db2 = cache.getDB(1, State.CLOSED,
            containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    Assert.assertEquals(2, db2.getReferenceCount());
    Assert.assertEquals(numCacheMisses + 1, metrics.getNumCacheMisses());
    Assert.assertEquals(2, db1.getReferenceCount());
    Assert.assertEquals(db1, db2);
    Assert.assertEquals(numDbGetCount + 2, metrics.getNumDbGetOps());
    Assert.assertEquals(numCacheMisses + 1, metrics.getNumCacheMisses());

    // add one more references to ContainerCache.
    ReferenceCountedDB db3 = cache.getDB(2, State.CLOSED,
            containerDir2.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    Assert.assertEquals(1, db3.getReferenceCount());

    // and close the reference
    db3.close();
    Assert.assertEquals(0, db3.getReferenceCount());

    // add one more reference to ContainerCache and verify that it will not
    // evict the least recent entry as it has reference.
    ReferenceCountedDB db4 = cache.getDB(3, State.CLOSED,
            containerDir3.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    Assert.assertEquals(1, db4.getReferenceCount());

    Assert.assertEquals(2, cache.size());
    Assert.assertNotNull(cache.get(containerDir1.getPath()));
    Assert.assertNull(cache.get(containerDir2.getPath()));

    // Now close both the references for container1
    db1.close();
    db2.close();
    Assert.assertEquals(0, db1.getReferenceCount());
    Assert.assertEquals(0, db2.getReferenceCount());


    // The reference count for container1 is 0 but it is not evicted.
    ReferenceCountedDB db5 = cache.getDB(1, State.CLOSED,
            containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    Assert.assertEquals(1, db5.getReferenceCount());
    Assert.assertEquals(db1, db5);
    db5.close();
    db4.close();


    // Decrementing reference count below zero should fail.
    thrown.expect(IllegalArgumentException.class);
    db5.close();
  }

  @Test
  public void testConcurrentDBGet() throws Exception {
    File root = new File(testRoot);
    root.mkdirs();
    root.deleteOnExit();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);
    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    Assert.assertEquals(0, cache.size());
    File containerDir = new File(root, "cont1");
    createContainerDB(conf, containerDir);
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Runnable task = () -> {
      try {
        ReferenceCountedDB db1 = cache.getDB(1, State.CLOSED,
            containerDir.getPath(),
            VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
        Assert.assertNotNull(db1);
      } catch (IOException e) {
        Assert.fail("Should get the DB instance");
      }
    };
    List<Future> futureList = new ArrayList<>();
    futureList.add(executorService.submit(task));
    futureList.add(executorService.submit(task));
    for (Future future: futureList) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        Assert.fail("Should get the DB instance");
      }
    }

    ReferenceCountedDB db = cache.getDB(1, State.CLOSED,
        containerDir.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    db.close();
    db.close();
    db.close();
    Assert.assertEquals(1, cache.size());
    db.cleanup();
  }

  @Test
  public void testOpenContainerCache() throws Exception {
    File root = new File(testRoot);
    root.mkdirs();

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);

    ContainerCache cache = ContainerCache.getInstance(conf);
    cache.clear();
    Assert.assertEquals(0, cache.size());
    File containerDir1 = new File(root, "cont1");
    File containerDir2 = new File(root, "cont2");

    ReferenceCountedDB db1 = createContainerDB(conf, containerDir1);
    ReferenceCountedDB db2 = createContainerDB(conf, containerDir2);

    ContainerCacheMetrics metrics = cache.getMetrics();

    long numOpenGetsFails = metrics.getNumOpenContainerGetsFailures();
    long numCacheHits = metrics.getNumCacheHits();

    long numOpenGetsSuccess = metrics.getNumOpenContainerGetsSuccess();

    try {
      cache.getDB(1, State.OPEN,
          containerDir1.getPath(),
          VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
      Assert.fail("This should fail as container is not added to Cache");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }

    Assert.assertEquals(numOpenGetsFails + 1,
        metrics.getNumOpenContainerGetsFailures());

    cache.addDB(containerDir1.getPath(), db1);
    cache.addDB(containerDir2.getPath(), db2);

    Assert.assertEquals(2, metrics.getNumOpenCacheEntries());

    ReferenceCountedDB db3 = cache.getDB(1, State.OPEN,
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);

    Assert.assertEquals(db1, db3);
    Assert.assertEquals(numOpenGetsSuccess + 1,
        metrics.getNumOpenContainerGetsSuccess());
    Assert.assertEquals(1, db1.getReferenceCount());
    Assert.assertEquals(0, db2.getReferenceCount());

    // Now mark container as closed
    cache.markContainerClosed(containerDir1.getPath());
    Assert.assertEquals(1, metrics.getNumOpenCacheEntries());

    ReferenceCountedDB db4 = cache.getDB(1, State.CLOSED,
        containerDir1.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    // Assert this is not read as open container
    Assert.assertEquals(numOpenGetsSuccess + 1,
        metrics.getNumOpenContainerGetsSuccess());
    // Assert this is read from close container cache
    Assert.assertEquals(numCacheHits + 1, metrics.getNumCacheHits());
    Assert.assertEquals(2, db4.getReferenceCount());
    Assert.assertEquals(0, db2.getReferenceCount());
    db3.close();
    db3.close();
    Assert.assertEquals(0, db2.getReferenceCount());

    // Now again open
    ReferenceCountedDB db5 = cache.getDB(2, State.OPEN,
        containerDir2.getPath(),
        VersionedDatanodeFeatures.SchemaV2.chooseSchemaVersion(), conf);
    db5.close();
    Assert.assertEquals(numOpenGetsSuccess + 2,
        metrics.getNumOpenContainerGetsSuccess());

    cache.shutdownCache();
  }
}
