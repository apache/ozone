package org.apache.hadoop.hdds.utils.db.managed;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryLoader;
import org.apache.ozone.test.tag.Native;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.apache.hadoop.hdds.utils.NativeConstants.ROCKS_TOOLS_NATIVE_LIBRARY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRocksObjectLeakDetector {

  @Native(ROCKS_TOOLS_NATIVE_LIBRARY_NAME)
  @Test
  public void testLeakDetector() throws Exception {
    Assumptions.assumeTrue(NativeLibraryLoader.getInstance().loadLibrary(ROCKS_TOOLS_NATIVE_LIBRARY_NAME));

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
    allocate(ManagedBloomFilter::new, true);
    System.gc();
    assertEquals(managedObjects + 1, ManagedRocksObjectMetrics.INSTANCE.totalManagedObjects());
    assertEquals(leakObjects, ManagedRocksObjectMetrics.INSTANCE.totalLeakObjects());

    // Allocate and not close.
    allocate(supplier, false);
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
