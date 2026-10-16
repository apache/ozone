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

package org.apache.hadoop.hdds.scm.container.placement.metrics;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import org.apache.hadoop.fs.StorageType;

/**
 * Interface that defines Node Stats.
 */
interface NodeStat {
  /**
   * Get capacity of the node.
   * @return capacity of the node.
   */
  LongMetric getCapacity();

  /**
   * Get the used space of the node.
   * @return the used space of the node.
   */
  LongMetric getScmUsed();

  /**
   * Get the remaining space of the node.
   * @return the remaining space of the node.
   */
  LongMetric getRemaining();

  /**
   * Get the committed space of the node.
   * @return the committed space of the node
   */
  LongMetric getCommitted();

  /**
   * Get a min free space available to spare on the node.
   * @return a min free space available to spare
   */
  LongMetric getFreeSpaceToSpare();

  /**
   * Get the reserved space on the node.
   * @return the reserved space on the node
   */
  LongMetric getReserved();

  /**
   * Get capacity of the node for a specific StorageType.
   * @return capacity of the node for a specific StorageType.
   */
  LongMetric getCapacity(StorageType storageType);

  /**
   * Get the used space of the node for a specific StorageType.
   * @return the used space of the node for a specific StorageType.
   */
  LongMetric getScmUsed(StorageType storageType);

  /**
   * Get the remaining space of the node for a specific StorageType.
   * @return the remaining space of the node for a specific StorageType.
   */
  LongMetric getRemaining(StorageType storageType);

  /**
   * Get the committed space of the node for a specific StorageType.
   * @return the committed space of the node for a specific StorageType.
   */
  LongMetric getCommitted(StorageType storageType);

  /**
   * Get the min free space available to spare on the node for a specific StorageType.
   * @return the min free space available to spare for a specific StorageType.
   */
  LongMetric getFreeSpaceToSpare(StorageType storageType);

  /**
   * Get the reserved space on the node for a specific StorageType.
   * @return the reserved space on the node for a specific StorageType.
   */
  LongMetric getReserved(StorageType storageType);

  /**
   * Copy the total and per-StorageType values from another stat.
   *
   * @param stat - stat to be set.
   */
  @VisibleForTesting
  void set(NodeStat stat);

  /**
   * Adding of the stat.
   * @param stat - stat to be added.
   * @return updated node stat.
   */
  NodeStat add(NodeStat stat);

  /**
   * Add the specified capacity, used, remaining, committed, freeSpaceToSpare and reserved
   * values for a specific StorageType.
   *
   * @param capacity   Capacity to add for the specified storage type.
   * @param used       Used space to add for the specified storage type.
   * @param remaining  Remaining space to add for the specified storage type.
   * @param committed  Committed space to add for the specified storage type.
   * @param freeSpaceToSpare  FreeSpaceToSpare space to add for the specified storage type.
   * @param reserved   Reserved space to add for the specified storage type.
   * @param storageType The storage type for the specified values.
   * @return Updated node stat.
   */
  NodeStat add(long capacity, long used, long remaining, long committed,
      long freeSpaceToSpare, long reserved, @Nonnull StorageType storageType);

  /**
   * Subtract of the stat.
   * @param stat - stat to be subtracted.
   * @return updated nodestat.
   */
  NodeStat subtract(NodeStat stat);
}
