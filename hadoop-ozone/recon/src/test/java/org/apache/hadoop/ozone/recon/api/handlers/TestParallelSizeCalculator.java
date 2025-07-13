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

package org.apache.hadoop.ozone.recon.api.handlers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for ParallelSizeCalculator.
 */
public class TestParallelSizeCalculator {

  private ReconNamespaceSummaryManager mockManager;
  private ParallelSizeCalculator parallelCalculator;
  private static final int PARALLELISM = 4;
  private static final int THRESHOLD = 2;

  @BeforeEach
  public void setUp() {
    mockManager = mock(ReconNamespaceSummaryManager.class);
    parallelCalculator = new ParallelSizeCalculator(mockManager, PARALLELISM, THRESHOLD);
  }

  @AfterEach
  public void tearDown() {
    if (parallelCalculator != null) {
      parallelCalculator.shutdown();
    }
  }

  @Test
  public void testSimpleCalculation() throws IOException {
    // Setup test data
    long rootId = 1L;
    long child1Id = 2L;
    long child2Id = 3L;
    
    // Root directory with 2 children
    Set<Long> rootChildren = new HashSet<>();
    rootChildren.add(child1Id);
    rootChildren.add(child2Id);
    
    NSSummary rootSummary = new NSSummary(5, 500L, new int[41], rootChildren, "root", 0);
    NSSummary child1Summary = new NSSummary(10, 1000L, new int[41], new HashSet<>(), "child1", rootId);
    NSSummary child2Summary = new NSSummary(15, 1500L, new int[41], new HashSet<>(), "child2", rootId);
    
    // Mock the manager calls
    when(mockManager.getNSSummary(rootId)).thenReturn(rootSummary);
    when(mockManager.getNSSummary(child1Id)).thenReturn(child1Summary);
    when(mockManager.getNSSummary(child2Id)).thenReturn(child2Summary);
    
    // Calculate total size
    long totalSize = parallelCalculator.calculateTotalSize(rootId);
    
    // Verify result: root (500) + child1 (1000) + child2 (1500) = 3000
    assertEquals(3000L, totalSize);
    
    // Verify performance metrics
    long[] metrics = parallelCalculator.getPerformanceMetrics();
    assertTrue(metrics[0] > 0, "Should have made RocksDB queries");
    assertTrue(metrics[1] > 0, "Should have executed tasks");
  }

  @Test
  public void testBatchedCalculation() throws IOException {
    // Setup test data
    long rootId = 1L;
    
    // Root directory with no children
    NSSummary rootSummary = new NSSummary(5, 500L, new int[41], new HashSet<>(), "root", 0);
    
    // Mock the manager calls
    when(mockManager.getNSSummary(rootId)).thenReturn(rootSummary);
    
    // Calculate total size using batched approach
    long totalSize = parallelCalculator.calculateTotalSizeBatched(rootId);
    
    // Verify result: just root (500)
    assertEquals(500L, totalSize);
  }

  @Test
  public void testNullSummary() throws IOException {
    // Setup test data
    long rootId = 1L;
    
    // Mock the manager to return null (non-existent directory)
    when(mockManager.getNSSummary(rootId)).thenReturn(null);
    
    // Calculate total size
    long totalSize = parallelCalculator.calculateTotalSize(rootId);
    
    // Verify result: should be 0 for non-existent directory
    assertEquals(0L, totalSize);
  }

  @Test
  public void testConfiguration() {
    assertEquals(PARALLELISM, parallelCalculator.getParallelism());
    assertEquals(THRESHOLD, parallelCalculator.getParallelThreshold());
    assertTrue(parallelCalculator.getForkJoinPool() != null);
  }

  @Test
  public void testInvalidParallelismValidation() {
    int expectedDefault = Math.max(1, Runtime.getRuntime().availableProcessors());
    
    // Test with parallelism = 0 (should be corrected to available processors)
    ParallelSizeCalculator calculator0 = new ParallelSizeCalculator(mockManager, 0, THRESHOLD);
    assertEquals(expectedDefault, calculator0.getParallelism());
    calculator0.shutdown();
    
    // Test with parallelism = -1 (should be corrected to available processors)
    ParallelSizeCalculator calculatorNegative = new ParallelSizeCalculator(mockManager, -1, THRESHOLD);
    assertEquals(expectedDefault, calculatorNegative.getParallelism());
    calculatorNegative.shutdown();
    
    // Test with valid parallelism (should remain unchanged)
    ParallelSizeCalculator calculatorValid = new ParallelSizeCalculator(mockManager, 4, THRESHOLD);
    assertEquals(4, calculatorValid.getParallelism());
    calculatorValid.shutdown();
  }
} 