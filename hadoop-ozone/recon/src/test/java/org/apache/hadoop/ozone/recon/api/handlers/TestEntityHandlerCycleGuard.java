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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.QuotaUsageResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests that the NSSummary tree walks in {@link EntityHandler} tolerate a
 * corrupted (self-referencing / cyclic) tree instead of crashing Recon with a
 * {@link StackOverflowError}.
 */
public class TestEntityHandlerCycleGuard {

  private final ReconNamespaceSummaryManager nsSummaryManager =
      mock(ReconNamespaceSummaryManager.class);

  private EntityHandler newHandler() {
    ReconOMMetadataManager omMetadataManager =
        mock(ReconOMMetadataManager.class);
    OzoneStorageContainerManager reconSCM =
        mock(OzoneStorageContainerManager.class);
    return new EntityHandler(nsSummaryManager, omMetadataManager, reconSCM,
        null, "/") {
      @Override
      public NamespaceSummaryResponse getSummaryResponse() {
        return null;
      }

      @Override
      public DUResponse getDuResponse(boolean listFile, boolean withReplica,
          boolean sort) {
        return null;
      }

      @Override
      public QuotaUsageResponse getQuotaResponse() {
        return null;
      }

      @Override
      public FileSizeDistributionResponse getDistResponse() {
        return null;
      }
    };
  }

  private NSSummary nsSummary(int numFiles, long size, Set<Long> children) {
    int[] bucket = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
    bucket[0] = numFiles;
    return new NSSummary(numFiles, size, size, bucket, children, "dir", 0);
  }

  @Test
  @Timeout(30)
  public void testSelfReferencingDirDoesNotOverflow() throws IOException {
    // Directory 1 lists itself as its own child: the corruption that would
    // otherwise recurse forever. Both walks must terminate.
    when(nsSummaryManager.getNSSummary(1L))
        .thenReturn(nsSummary(2, 100L, newSet(1L)));

    EntityHandler handler = newHandler();
    assertEquals(0, handler.getTotalDirCount(1L));
    int[] dist = handler.getTotalFileSizeDist(1L);
    assertEquals(2, dist[0]);
    verify(nsSummaryManager, times(2))
        .recordNSSummaryInvalidTreeDetection();
  }

  @Test
  @Timeout(30)
  public void testCyclicTreeCountsEachDirOnce() throws IOException {
    // 1 -> 2 -> 3 -> 1 (back edge to the root) and 2 -> 2 (self loop).
    when(nsSummaryManager.getNSSummary(1L))
        .thenReturn(nsSummary(1, 10L, newSet(2L)));
    when(nsSummaryManager.getNSSummary(2L))
        .thenReturn(nsSummary(1, 20L, newSet(3L, 2L)));
    when(nsSummaryManager.getNSSummary(3L))
        .thenReturn(nsSummary(1, 30L, newSet(1L)));

    EntityHandler handler = newHandler();
    // Reachable directories other than the root object 1 are {2, 3}.
    assertEquals(2, handler.getTotalDirCount(1L));
    // Each distinct directory contributes its file count exactly once.
    assertEquals(3, handler.getTotalFileSizeDist(1L)[0]);
    verify(nsSummaryManager, times(2))
        .recordNSSummaryInvalidTreeDetection();
  }

  @Test
  @Timeout(30)
  public void testCleanTreeUnaffected() throws IOException {
    // 1 -> {2, 3}, 2 -> {4}. No cycles: counts match the pre-fix behavior.
    when(nsSummaryManager.getNSSummary(1L))
        .thenReturn(nsSummary(1, 10L, newSet(2L, 3L)));
    when(nsSummaryManager.getNSSummary(2L))
        .thenReturn(nsSummary(1, 20L, newSet(4L)));
    when(nsSummaryManager.getNSSummary(3L))
        .thenReturn(nsSummary(1, 30L, newSet()));
    when(nsSummaryManager.getNSSummary(4L))
        .thenReturn(nsSummary(1, 40L, newSet()));

    EntityHandler handler = newHandler();
    assertEquals(3, handler.getTotalDirCount(1L));
    assertThat(handler.getTotalFileSizeDist(1L)[0]).isEqualTo(4);
  }

  @Test
  @Timeout(30)
  public void testWideTreeIsTraversedIncrementally() throws IOException {
    int childCount = 10_000;
    AtomicInteger generatedChildren = new AtomicInteger();
    AtomicInteger childLookups = new AtomicInteger();
    Set<Long> children = new AbstractSet<Long>() {
      @Override
      public Iterator<Long> iterator() {
        return new Iterator<Long>() {
          private long nextId = 2;

          @Override
          public boolean hasNext() {
            return nextId <= childCount + 1L;
          }

          @Override
          public Long next() {
            assertEquals(childLookups.get(), generatedChildren.get(),
                "Tree walk buffered child IDs before reading their NSSummary");
            generatedChildren.incrementAndGet();
            return nextId++;
          }
        };
      }

      @Override
      public int size() {
        return childCount;
      }
    };

    when(nsSummaryManager.getNSSummary(anyLong())).thenAnswer(invocation -> {
      long objectId = invocation.getArgument(0);
      if (objectId == 1L) {
        return nsSummary(0, 0L, children);
      }
      childLookups.incrementAndGet();
      return null;
    });

    assertEquals(childCount, newHandler().getTotalDirCount(1L));
    assertEquals(childCount, childLookups.get());
  }

  private static Set<Long> newSet(Long... ids) {
    return new HashSet<>(Arrays.asList(ids));
  }
}
