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

package org.apache.ozone.rocksdiff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for FlushLinkedList.
 */
public class TestFlushLinkedList {

  private FlushLinkedList flushList;

  @BeforeEach
  public void setup() {
    flushList = new FlushLinkedList();
  }

  @Test
  public void testAddFlush() {
    assertTrue(flushList.isEmpty());
    assertEquals(0, flushList.size());

    flushList.addFlush("000123.sst", 1000L, System.currentTimeMillis(),
        "key1", "key999", "keyTable");

    assertFalse(flushList.isEmpty());
    assertEquals(1, flushList.size());

    FlushNode node = flushList.getFlushNode("000123.sst");
    assertNotNull(node);
    assertEquals("000123.sst", node.getFileName());
    assertEquals(1000L, node.getSnapshotGeneration());
  }

  @Test
  public void testGetFlushNode() {
    flushList.addFlush("000123.sst", 1000L, System.currentTimeMillis(),
        "key1", "key999", "keyTable");
    flushList.addFlush("000456.sst", 2000L, System.currentTimeMillis(),
        "key1", "key999", "directoryTable");

    FlushNode node1 = flushList.getFlushNode("000123.sst");
    assertNotNull(node1);
    assertEquals("000123.sst", node1.getFileName());

    FlushNode node2 = flushList.getFlushNode("000456.sst");
    assertNotNull(node2);
    assertEquals("000456.sst", node2.getFileName());

    FlushNode nonExistent = flushList.getFlushNode("999999.sst");
    assertNull(nonExistent);
  }

  @Test
  public void testGetFlushNodes() {
    long time = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, time, "a", "b", "cf1");
    flushList.addFlush("000002.sst", 2000L, time + 1000, "c", "d", "cf2");
    flushList.addFlush("000003.sst", 3000L, time + 2000, "e", "f", "cf3");

    List<FlushNode> nodes = flushList.getFlushNodes();
    assertEquals(3, nodes.size());

    // Verify order (oldest first)
    assertEquals("000001.sst", nodes.get(0).getFileName());
    assertEquals("000002.sst", nodes.get(1).getFileName());
    assertEquals("000003.sst", nodes.get(2).getFileName());
  }

  @Test
  public void testGetFlushNodesBetween() {
    long time = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, time, "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, time + 1000, "c", "d", "cf");
    flushList.addFlush("000003.sst", 3000L, time + 2000, "e", "f", "cf");
    flushList.addFlush("000004.sst", 4000L, time + 3000, "g", "h", "cf");
    flushList.addFlush("000005.sst", 5000L, time + 4000, "i", "j", "cf");

    // Get nodes between generation 2000 and 4000 (inclusive)
    List<FlushNode> nodes = flushList.getFlushNodesBetween(2000L, 4000L);
    assertEquals(3, nodes.size());
    assertEquals("000002.sst", nodes.get(0).getFileName());
    assertEquals("000003.sst", nodes.get(1).getFileName());
    assertEquals("000004.sst", nodes.get(2).getFileName());

    // Get all nodes
    List<FlushNode> allNodes = flushList.getFlushNodesBetween(0L, 10000L);
    assertEquals(5, allNodes.size());

    // Get no nodes (range before all generations)
    List<FlushNode> noNodes = flushList.getFlushNodesBetween(0L, 500L);
    assertEquals(0, noNodes.size());

    // Get single node
    List<FlushNode> singleNode = flushList.getFlushNodesBetween(3000L, 3000L);
    assertEquals(1, singleNode.size());
    assertEquals("000003.sst", singleNode.get(0).getFileName());
  }

  @Test
  public void testPruneOlderThan() {
    long time = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, time, "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, time + 1000, "c", "d", "cf");
    flushList.addFlush("000003.sst", 3000L, time + 2000, "e", "f", "cf");
    flushList.addFlush("000004.sst", 4000L, time + 3000, "g", "h", "cf");
    flushList.addFlush("000005.sst", 5000L, time + 4000, "i", "j", "cf");

    assertEquals(5, flushList.size());

    // Prune all nodes older than generation 3000
    List<String> prunedFiles = flushList.pruneOlderThan(3000L);
    assertEquals(2, prunedFiles.size());
    assertTrue(prunedFiles.contains("000001.sst"));
    assertTrue(prunedFiles.contains("000002.sst"));

    // Verify remaining nodes
    assertEquals(3, flushList.size());
    assertNull(flushList.getFlushNode("000001.sst"));
    assertNull(flushList.getFlushNode("000002.sst"));
    assertNotNull(flushList.getFlushNode("000003.sst"));
    assertNotNull(flushList.getFlushNode("000004.sst"));
    assertNotNull(flushList.getFlushNode("000005.sst"));

    // Verify list order is maintained
    List<FlushNode> remainingNodes = flushList.getFlushNodes();
    assertEquals("000003.sst", remainingNodes.get(0).getFileName());
    assertEquals("000004.sst", remainingNodes.get(1).getFileName());
    assertEquals("000005.sst", remainingNodes.get(2).getFileName());
  }

  @Test
  public void testPruneOlderThanWithNoMatch() {
    flushList.addFlush("000001.sst", 5000L, System.currentTimeMillis(),
        "a", "b", "cf");
    flushList.addFlush("000002.sst", 6000L, System.currentTimeMillis(),
        "c", "d", "cf");

    // Prune older than generation 1000 (no nodes should be pruned)
    List<String> prunedFiles = flushList.pruneOlderThan(1000L);
    assertEquals(0, prunedFiles.size());
    assertEquals(2, flushList.size());
  }

  @Test
  public void testGetOldestAndNewest() {
    assertTrue(flushList.isEmpty());
    assertNull(flushList.getOldest());
    assertNull(flushList.getNewest());

    long time = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, time, "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, time + 1000, "c", "d", "cf");
    flushList.addFlush("000003.sst", 3000L, time + 2000, "e", "f", "cf");

    FlushNode oldest = flushList.getOldest();
    assertNotNull(oldest);
    assertEquals("000001.sst", oldest.getFileName());
    assertEquals(1000L, oldest.getSnapshotGeneration());

    FlushNode newest = flushList.getNewest();
    assertNotNull(newest);
    assertEquals("000003.sst", newest.getFileName());
    assertEquals(3000L, newest.getSnapshotGeneration());
  }

  @Test
  public void testClear() {
    flushList.addFlush("000001.sst", 1000L, System.currentTimeMillis(),
        "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, System.currentTimeMillis(),
        "c", "d", "cf");

    assertEquals(2, flushList.size());
    assertFalse(flushList.isEmpty());

    flushList.clear();

    assertEquals(0, flushList.size());
    assertTrue(flushList.isEmpty());
    assertNull(flushList.getFlushNode("000001.sst"));
    assertNull(flushList.getFlushNode("000002.sst"));
  }

  @Test
  public void testTimeOrdering() {
    // Add nodes with different flush times but ensure they maintain insertion order
    long baseTime = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, baseTime + 1000, "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, baseTime + 500, "c", "d", "cf");
    flushList.addFlush("000003.sst", 3000L, baseTime + 1500, "e", "f", "cf");

    List<FlushNode> nodes = flushList.getFlushNodes();
    assertEquals(3, nodes.size());

    // Should be in insertion order (which represents time order in real usage)
    assertEquals("000001.sst", nodes.get(0).getFileName());
    assertEquals("000002.sst", nodes.get(1).getFileName());
    assertEquals("000003.sst", nodes.get(2).getFileName());
  }

  @Test
  public void testConcurrentMapAccess() {
    flushList.addFlush("000001.sst", 1000L, System.currentTimeMillis(),
        "a", "b", "cf");

    // Get the concurrent map and verify it contains the node
    assertNotNull(flushList.getFlushNodeMap());
    assertEquals(1, flushList.getFlushNodeMap().size());
    assertTrue(flushList.getFlushNodeMap().containsKey("000001.sst"));
  }

  @Test
  public void testPruneOlderThanTime() {
    long baseTime = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 1000L, baseTime - 5000, "a", "b", "cf");
    flushList.addFlush("000002.sst", 2000L, baseTime - 4000, "c", "d", "cf");
    flushList.addFlush("000003.sst", 3000L, baseTime - 3000, "e", "f", "cf");
    flushList.addFlush("000004.sst", 4000L, baseTime - 2000, "g", "h", "cf");
    flushList.addFlush("000005.sst", 5000L, baseTime - 1000, "i", "j", "cf");

    assertEquals(5, flushList.size());

    // Prune all nodes older than baseTime - 3000
    long cutoffTime = baseTime - 3000;
    List<String> prunedFiles = flushList.pruneOlderThanTime(cutoffTime);

    // Should have pruned file1 and file2 (their flushTime < cutoffTime)
    assertEquals(2, prunedFiles.size());
    assertTrue(prunedFiles.contains("000001.sst"));
    assertTrue(prunedFiles.contains("000002.sst"));

    // Verify remaining nodes
    assertEquals(3, flushList.size());
    assertNull(flushList.getFlushNode("000001.sst"));
    assertNull(flushList.getFlushNode("000002.sst"));
    assertNotNull(flushList.getFlushNode("000003.sst"));
    assertNotNull(flushList.getFlushNode("000004.sst"));
    assertNotNull(flushList.getFlushNode("000005.sst"));

    // Verify list order is maintained
    List<FlushNode> remainingNodes = flushList.getFlushNodes();
    assertEquals("000003.sst", remainingNodes.get(0).getFileName());
    assertEquals("000004.sst", remainingNodes.get(1).getFileName());
    assertEquals("000005.sst", remainingNodes.get(2).getFileName());
  }

  @Test
  public void testPruneOlderThanTimeWithNoMatch() {
    long baseTime = System.currentTimeMillis();
    flushList.addFlush("000001.sst", 5000L, baseTime, "a", "b", "cf");
    flushList.addFlush("000002.sst", 6000L, baseTime + 1000, "c", "d", "cf");

    // Prune older than a time before all nodes
    List<String> prunedFiles = flushList.pruneOlderThanTime(baseTime - 5000);
    assertEquals(0, prunedFiles.size());
    assertEquals(2, flushList.size());
  }

  @Test
  public void testPruneOlderThanTimeMixedOrder() {
    long baseTime = System.currentTimeMillis();
    // Add with non-sequential generations but time-ordered flushTime
    flushList.addFlush("000001.sst", 5000L, baseTime, "a", "b", "cf");
    flushList.addFlush("000002.sst", 3000L, baseTime + 1000, "c", "d", "cf");
    flushList.addFlush("000003.sst", 7000L, baseTime + 2000, "e", "f", "cf");

    // Prune based on time, not generation
    long cutoffTime = baseTime + 1500;
    List<String> prunedFiles = flushList.pruneOlderThanTime(cutoffTime);

    // Should prune file1 and file2 based on flushTime
    assertEquals(2, prunedFiles.size());
    assertTrue(prunedFiles.contains("000001.sst"));
    assertTrue(prunedFiles.contains("000002.sst"));

    // Only file3 should remain
    assertEquals(1, flushList.size());
    assertNotNull(flushList.getFlushNode("000003.sst"));
  }
}
