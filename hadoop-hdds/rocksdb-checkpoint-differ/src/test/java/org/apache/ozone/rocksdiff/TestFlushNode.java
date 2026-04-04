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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

/**
 * Test class for FlushNode.
 */
public class TestFlushNode {

  @Test
  public void testFlushNodeCreation() {
    String fileName = "000123.sst";
    long snapshotGeneration = 1000L;
    long flushTime = System.currentTimeMillis();
    String startKey = "key001";
    String endKey = "key999";
    String columnFamily = "keyTable";

    FlushNode node = new FlushNode(fileName, snapshotGeneration, flushTime,
        startKey, endKey, columnFamily);

    assertNotNull(node);
    assertEquals(fileName, node.getFileName());
    assertEquals(snapshotGeneration, node.getSnapshotGeneration());
    assertEquals(flushTime, node.getFlushTime());
    assertEquals(startKey, node.getStartKey());
    assertEquals(endKey, node.getEndKey());
    assertEquals(columnFamily, node.getColumnFamily());
  }

  @Test
  public void testFlushNodeWithNullKeyRanges() {
    String fileName = "000456.sst";
    long snapshotGeneration = 2000L;
    long flushTime = System.currentTimeMillis();

    FlushNode node = new FlushNode(fileName, snapshotGeneration, flushTime,
        null, null, null);

    assertNotNull(node);
    assertEquals(fileName, node.getFileName());
    assertEquals(snapshotGeneration, node.getSnapshotGeneration());
    assertEquals(flushTime, node.getFlushTime());
    assertEquals(null, node.getStartKey());
    assertEquals(null, node.getEndKey());
    assertEquals(null, node.getColumnFamily());
  }

  @Test
  public void testFlushNodeIdentityEquality() {
    // FlushNode uses identity-based equality (like CompactionNode)
    String fileName = "000789.sst";
    long snapshotGeneration = 3000L;
    long flushTime = System.currentTimeMillis();

    FlushNode node1 = new FlushNode(fileName, snapshotGeneration, flushTime,
        "start", "end", "cf");
    FlushNode node2 = new FlushNode(fileName, snapshotGeneration, flushTime,
        "start", "end", "cf");

    // Identity equality: same object reference
    assertSame(node1, node1);
    assertEquals(node1, node1);

    // Different objects are not equal, even with same content
    assertNotEquals(node1, node2);
  }

  @Test
  public void testFlushNodeHashCode() {
    // Hash code is based on fileName only
    String fileName = "000321.sst";
    long snapshotGeneration1 = 4000L;
    long snapshotGeneration2 = 5000L;
    long flushTime1 = System.currentTimeMillis();
    long flushTime2 = flushTime1 + 1000;

    FlushNode node1 = new FlushNode(fileName, snapshotGeneration1, flushTime1,
        "start1", "end1", "cf1");
    FlushNode node2 = new FlushNode(fileName, snapshotGeneration2, flushTime2,
        "start2", "end2", "cf2");

    // Same fileName -> same hashCode
    assertEquals(node1.hashCode(), node2.hashCode());
  }

  @Test
  public void testFlushNodeToString() {
    String fileName = "000654.sst";
    long snapshotGeneration = 6000L;
    long flushTime = 1234567890L;
    String columnFamily = "directoryTable";

    FlushNode node = new FlushNode(fileName, snapshotGeneration, flushTime,
        "startKey", "endKey", columnFamily);

    String toString = node.toString();
    assertNotNull(toString);
    // Verify the string contains key information
    assert toString.contains(fileName);
    assert toString.contains(String.valueOf(snapshotGeneration));
    assert toString.contains(String.valueOf(flushTime));
    assert toString.contains(columnFamily);
  }

  @Test
  public void testFlushNodeWithDifferentFileNames() {
    long snapshotGeneration = 7000L;
    long flushTime = System.currentTimeMillis();

    FlushNode node1 = new FlushNode("000111.sst", snapshotGeneration, flushTime,
        "a", "b", "cf");
    FlushNode node2 = new FlushNode("000222.sst", snapshotGeneration, flushTime,
        "a", "b", "cf");

    // Different file names -> different hash codes
    assertNotEquals(node1.hashCode(), node2.hashCode());
  }
}
