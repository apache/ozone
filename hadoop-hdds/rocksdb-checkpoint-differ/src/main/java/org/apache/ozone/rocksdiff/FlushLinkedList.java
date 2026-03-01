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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple linked list structure for tracking L0 SST files created by flush operations.
 * This is a simpler replacement for CompactionDag that only tracks flush operations
 * instead of the full compaction graph.
 */
public class FlushLinkedList {
  private static final Logger LOG = LoggerFactory.getLogger(FlushLinkedList.class);

  // Time-ordered list of flush operations (oldest first)
  private final LinkedList<FlushNode> flushList = new LinkedList<>();

  // Lookup map for fileName -> FlushNode.
  // All access is protected by synchronized methods.
  private final ConcurrentMap<String, FlushNode> flushNodeMap = new ConcurrentHashMap<>();

  /**
   * Add a new flush node to the linked list.
   * Nodes are appended in the order they are added (time-ordered).
   *
   * @param fileName           SST file name
   * @param snapshotGeneration DB sequence number when flush occurred
   * @param flushTime          Timestamp when flush completed (milliseconds)
   * @param startKey           Smallest key in the SST file
   * @param endKey             Largest key in the SST file
   * @param columnFamily       Column family name
   */
  public synchronized void addFlush(String fileName,
                                    long snapshotGeneration,
                                    long flushTime,
                                    String startKey,
                                    String endKey,
                                    String columnFamily) {
    FlushNode node = new FlushNode(fileName, snapshotGeneration, flushTime,
        startKey, endKey, columnFamily);

    flushList.addLast(node);
    flushNodeMap.put(fileName, node);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Added flush node: {}", node);
    }
  }

  /**
   * Get a flush node by file name.
   *
   * @param fileName SST file name to lookup
   * @return FlushNode if found, null otherwise
   */
  public FlushNode getFlushNode(String fileName) {
    return flushNodeMap.get(fileName);
  }

  /**
   * Get all flush nodes in time order (oldest first).
   *
   * @return Unmodifiable view of the flush list
   */
  public synchronized List<FlushNode> getFlushNodes() {
    return new ArrayList<>(flushList);
  }

  /**
   * Get all flush nodes between two snapshot generations (inclusive).
   *
   * @param fromGeneration Starting snapshot generation (inclusive)
   * @param toGeneration   Ending snapshot generation (inclusive)
   * @return List of FlushNodes in the specified range
   */
  public synchronized List<FlushNode> getFlushNodesBetween(
      long fromGeneration, long toGeneration) {
    List<FlushNode> result = new ArrayList<>();

    for (FlushNode node : flushList) {
      long gen = node.getSnapshotGeneration();
      if (gen >= fromGeneration && gen <= toGeneration) {
        result.add(node);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found {} flush nodes between generations {} and {}",
          result.size(), fromGeneration, toGeneration);
    }

    return result;
  }

  /**
   * Prune (remove) all flush nodes older than the specified snapshot generation.
   * This is used to clean up old SST file references that are no longer needed.
   *
   * @param snapshotGeneration Nodes older than this generation are removed
   * @return List of file names that were pruned
   */
  public synchronized List<String> pruneOlderThan(long snapshotGeneration) {
    List<String> prunedFiles = new ArrayList<>();
    Iterator<FlushNode> iterator = flushList.iterator();

    while (iterator.hasNext()) {
      FlushNode node = iterator.next();
      if (node.getSnapshotGeneration() < snapshotGeneration) {
        iterator.remove();
        flushNodeMap.remove(node.getFileName());
        prunedFiles.add(node.getFileName());
      } else {
        // Since list is time-ordered, we can stop once we hit a newer node
        break;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pruned {} flush nodes older than generation {}",
          prunedFiles.size(), snapshotGeneration);
    }

    return prunedFiles;
  }

  /**
   * Prune (remove) all flush nodes older than the specified timestamp.
   * This is used to clean up old SST file references based on flush time
   * rather than snapshot generation.
   *
   * @param cutoffTime Nodes with flushTime older than this timestamp are removed (milliseconds)
   * @return List of file names that were pruned
   */
  public synchronized List<String> pruneOlderThanTime(long cutoffTime) {
    List<String> prunedFiles = new ArrayList<>();
    Iterator<FlushNode> iterator = flushList.iterator();

    while (iterator.hasNext()) {
      FlushNode node = iterator.next();
      if (node.getFlushTime() < cutoffTime) {
        iterator.remove();
        flushNodeMap.remove(node.getFileName());
        prunedFiles.add(node.getFileName());
      } else {
        // Since list is time-ordered, we can stop once we hit a newer node
        break;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pruned {} flush nodes older than time {}",
          prunedFiles.size(), cutoffTime);
    }

    return prunedFiles;
  }

  /**
   * Get the total number of flush nodes currently tracked.
   *
   * @return Size of the flush list
   */
  public synchronized int size() {
    return flushList.size();
  }

  /**
   * Check if the flush list is empty.
   *
   * @return true if no flush nodes are tracked
   */
  public synchronized boolean isEmpty() {
    return flushList.isEmpty();
  }

  /**
   * Get the oldest flush node in the list.
   *
   * @return First FlushNode or null if list is empty
   */
  public synchronized FlushNode getOldest() {
    return flushList.isEmpty() ? null : flushList.getFirst();
  }

  /**
   * Get the newest flush node in the list.
   *
   * @return Last FlushNode or null if list is empty
   */
  public synchronized FlushNode getNewest() {
    return flushList.isEmpty() ? null : flushList.getLast();
  }

  /**
   * Get the internal map for testing purposes.
   *
   * @return The flush node map
   */
  public ConcurrentMap<String, FlushNode> getFlushNodeMap() {
    return flushNodeMap;
  }

  /**
   * Clear all flush nodes from the list.
   * Used primarily for testing.
   */
  public synchronized void clear() {
    flushList.clear();
    flushNodeMap.clear();
    LOG.debug("Cleared all flush nodes");
  }
}
