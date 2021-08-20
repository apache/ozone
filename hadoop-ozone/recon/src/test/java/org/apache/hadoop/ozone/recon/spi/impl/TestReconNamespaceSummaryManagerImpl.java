/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi.impl;

import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test for NSSummary manager.
 */
public class TestReconNamespaceSummaryManagerImpl {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static ReconNamespaceSummaryManagerImpl reconNamespaceSummaryManager;
  private static int[] testBucket;
  private static final Set<Long> TEST_CHILD_DIR =
          new HashSet<>(Arrays.asList(new Long[]{1L, 2L, 3L}));

  @BeforeClass
  public static void setupOnce() throws Exception {
    ReconTestInjector reconTestInjector =
            new ReconTestInjector.Builder(TEMP_FOLDER)
                    .withReconSqlDb()
                    .withContainerDB()
                    .build();
    reconNamespaceSummaryManager = reconTestInjector.getInstance(
            ReconNamespaceSummaryManagerImpl.class);
    testBucket = new int[40];
    for (int i = 0; i < 40; ++i) {
      testBucket[i] = i + 1;
    }
  }

  @Before
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
    Assert.assertEquals(1, summary.getNumOfFiles());
    Assert.assertEquals(2, summary.getSizeOfFiles());
    Assert.assertEquals(3, summary2.getNumOfFiles());
    Assert.assertEquals(4, summary2.getSizeOfFiles());
    Assert.assertEquals(5, summary3.getNumOfFiles());
    Assert.assertEquals(6, summary3.getSizeOfFiles());

    Assert.assertEquals("dir1", summary.getDirName());
    Assert.assertEquals("dir2", summary2.getDirName());
    Assert.assertEquals("dir3", summary3.getDirName());

    // test child dir is written
    Assert.assertEquals(3, summary.getChildDir().size());
    // non-existent key
    Assert.assertNull(reconNamespaceSummaryManager.getNSSummary(0L));
  }

  @Test
  public void testInitNSSummaryTable() throws IOException {
    putThreeNSMetadata();
    Assert.assertFalse(
            reconNamespaceSummaryManager.getNSSummaryTable().isEmpty());
    reconNamespaceSummaryManager.clearNSSummaryTable();
    Assert.assertTrue(
            reconNamespaceSummaryManager.getNSSummaryTable().isEmpty());
  }

  private void putThreeNSMetadata() throws IOException {
    HashMap<Long, NSSummary> hmap = new HashMap<>();
    hmap.put(1L, new NSSummary(1, 2, testBucket, TEST_CHILD_DIR, "dir1"));
    hmap.put(2L, new NSSummary(3, 4, testBucket, TEST_CHILD_DIR, "dir2"));
    hmap.put(3L, new NSSummary(5, 6, testBucket, TEST_CHILD_DIR, "dir3"));
    for (Map.Entry entry: hmap.entrySet()) {
      reconNamespaceSummaryManager.storeNSSummary(
              (long)entry.getKey(), (NSSummary)entry.getValue());
    }
  }
}
