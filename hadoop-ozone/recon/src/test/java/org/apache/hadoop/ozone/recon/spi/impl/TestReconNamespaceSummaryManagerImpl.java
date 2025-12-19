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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for NSSummary manager.
 */
public class TestReconNamespaceSummaryManagerImpl {
  @TempDir()
  private static Path temporaryFolder;
  private static ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private static int[] testBucket;
  private static final Set<Long> TEST_CHILD_DIR =
          new HashSet<>(Arrays.asList(new Long[]{1L, 2L, 3L}));

  @BeforeAll
  public static void setupOnce() throws Exception {
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    ReconTestInjector reconTestInjector =
            new ReconTestInjector.Builder(temporaryFolder.toFile())
                    .withReconSqlDb()
                    .withReconOm(reconOMMetadataManager)
                    .withContainerDB()
                    .build();
    reconNamespaceSummaryManager = reconTestInjector.getInstance(
            ReconNamespaceSummaryManagerImpl.class);
    testBucket = new int[40];
    for (int i = 0; i < 40; ++i) {
      testBucket[i] = i + 1;
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    // Clear namespace table before running each test
    reconNamespaceSummaryManager.clearNSSummaryTable();
  }

  @Test
  public void testStoreAndGet() throws Exception {
    putThreeNSMetadata();
    NSSummary summary = reconNamespaceSummaryManager.getNSSummary(1L);
    NSSummary summary2 = reconNamespaceSummaryManager.getNSSummary(2L);
    NSSummary summary3 = reconNamespaceSummaryManager.getNSSummary(3L);
    assertEquals(1, summary.getNumOfFiles());
    assertEquals(2, summary.getSizeOfFiles());
    assertEquals(3, summary2.getNumOfFiles());
    assertEquals(4, summary2.getSizeOfFiles());
    assertEquals(5, summary3.getNumOfFiles());
    assertEquals(6, summary3.getSizeOfFiles());

    assertEquals("dir1", summary.getDirName());
    assertEquals("dir2", summary2.getDirName());
    assertEquals("dir3", summary3.getDirName());

    // test child dir is written
    assertEquals(3, summary.getChildDir().size());
    // non-existent key
    assertNull(reconNamespaceSummaryManager.getNSSummary(0L));
  }

  @Test
  public void testInitNSSummaryTable() throws IOException {
    putThreeNSMetadata();
    assertFalse(reconNamespaceSummaryManager.getNSSummaryTable().isEmpty());
    reconNamespaceSummaryManager.clearNSSummaryTable();
    assertTrue(reconNamespaceSummaryManager.getNSSummaryTable().isEmpty());
  }

  private void putThreeNSMetadata() throws IOException {
    HashMap<Long, NSSummary> hmap = new HashMap<>();
    hmap.put(1L, new NSSummary(1, 2, 2 * 3, testBucket, TEST_CHILD_DIR, "dir1", -1));
    hmap.put(2L, new NSSummary(3, 4, 4 * 3, testBucket, TEST_CHILD_DIR, "dir2", -1));
    hmap.put(3L, new NSSummary(5, 6, 6 * 3, testBucket, TEST_CHILD_DIR, "dir3", -1));
    RDBBatchOperation rdbBatchOperation = new RDBBatchOperation();
    for (Map.Entry entry: hmap.entrySet()) {
      reconNamespaceSummaryManager.batchStoreNSSummaries(rdbBatchOperation,
              (long)entry.getKey(), (NSSummary)entry.getValue());
    }
    reconNamespaceSummaryManager.commitBatchOperation(rdbBatchOperation);
  }
}
