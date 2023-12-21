package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omServiceId = "omServiceId1";
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(1)
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
    testLeakDetector(() -> new ManagedLRUCache(1l));
    testLeakDetector(ManagedOptions::new);
    testLeakDetector(ManagedReadOptions::new);
    testLeakDetector(() -> new ManagedSlice(RandomUtils.nextBytes(10)));
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
