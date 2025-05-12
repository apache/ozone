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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_ROCKSDB_STATISTICS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test managed rocks object leak detection.
 * This test creates garbage that will fail other tests and is intended for manual run only.
 * It is also flaky because of depending on background processes and environment (other background tasks
 * can create extra managed rocks objects and thus fails the counter assertions).
 */
@Unhealthy
public class TestRocksObjectLeakDetector {

  private static MiniOzoneCluster cluster;

  @BeforeAll
  static void setUp() throws IOException, InterruptedException,
      TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_STORE_ROCKSDB_STATISTICS, "ALL");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  static void cleanUp() {
    if (cluster != null) {
      assertThrows(AssertionError.class, () -> cluster.shutdown());
    }
  }

  @Test
  public void testLeakDetector() throws Exception {
    testLeakDetector(ManagedBloomFilter::new);
    testLeakDetector(ManagedColumnFamilyOptions::new);
    testLeakDetector(ManagedEnvOptions::new);
    testLeakDetector(ManagedFlushOptions::new);
    testLeakDetector(ManagedIngestExternalFileOptions::new);
    testLeakDetector(() -> new ManagedLRUCache(1L));
    testLeakDetector(ManagedOptions::new);
    testLeakDetector(ManagedReadOptions::new);
    testLeakDetector(() -> new ManagedSlice(RandomUtils.secure().randomBytes(10)));
    testLeakDetector(ManagedStatistics::new);
    testLeakDetector(ManagedWriteBatch::new);
    testLeakDetector(ManagedWriteOptions::new);
  }

  private <T extends AutoCloseable> void testLeakDetector(Supplier<T> supplier) throws Exception {
    // base metrics
    long managedObjects = ManagedRocksObjectMetrics.INSTANCE.totalManagedObjects();
    long leakObjects = ManagedRocksObjectMetrics.INSTANCE.totalLeakObjects();

    // Allocate and close.
    allocate(supplier, true);
    System.gc();
    // it could take a while for leak detection to run in the background.
    Thread.sleep(500);
    assertEquals(managedObjects + 1, ManagedRocksObjectMetrics.INSTANCE.totalManagedObjects());
    assertEquals(leakObjects, ManagedRocksObjectMetrics.INSTANCE.totalLeakObjects());

    // Allocate and not close.
    allocate(supplier, false);
    System.gc();
    Thread.sleep(500);
    assertEquals(managedObjects + 2, ManagedRocksObjectMetrics.INSTANCE.totalManagedObjects());
    assertEquals(leakObjects + 1, ManagedRocksObjectMetrics.INSTANCE.totalLeakObjects());
  }

  private <T extends AutoCloseable> void allocate(Supplier<T> supplier, boolean close) throws Exception {
    T object = supplier.get();
    if (close) {
      object.close();
    }
  }
}
