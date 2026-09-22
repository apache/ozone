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
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import java.util.EnumMap;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;

/**
 * This class represents the SCM node stat.
 */
public class SCMNodeStat implements NodeStat {
  private LongMetric capacity;
  private LongMetric scmUsed;
  private LongMetric remaining;
  private LongMetric committed;
  private LongMetric freeSpaceToSpare;
  private LongMetric reserved;

  private Map<StorageType, LongMetric> capacityPerStorageType = new EnumMap<>(StorageType.class);
  private Map<StorageType, LongMetric> usedPerStorageType = new EnumMap<>(StorageType.class);
  private Map<StorageType, LongMetric> remainingPerStorageType = new EnumMap<>(StorageType.class);
  private Map<StorageType, LongMetric> committedPerStorageType = new EnumMap<>(StorageType.class);
  private Map<StorageType, LongMetric> freeSpaceToSparePerStorageType = new EnumMap<>(StorageType.class);
  private Map<StorageType, LongMetric> reservedPerStorageType = new EnumMap<>(StorageType.class);

  public SCMNodeStat() {
    this.capacity = new LongMetric(0L);
    this.scmUsed = new LongMetric(0L);
    this.remaining = new LongMetric(0L);
    this.committed = new LongMetric(0L);
    this.freeSpaceToSpare = new LongMetric(0L);
    this.reserved = new LongMetric(0L);
    for (StorageType type : StorageType.values()) {
      capacityPerStorageType.put(type, new LongMetric(0L));
      usedPerStorageType.put(type, new LongMetric(0L));
      remainingPerStorageType.put(type, new LongMetric(0L));
      committedPerStorageType.put(type, new LongMetric(0L));
      freeSpaceToSparePerStorageType.put(type, new LongMetric(0L));
      reservedPerStorageType.put(type, new LongMetric(0L));
    }
  }

  public SCMNodeStat(SCMNodeStat other) {
    this.capacity = new LongMetric(other.capacity.get());
    this.scmUsed = new LongMetric(other.scmUsed.get());
    this.remaining = new LongMetric(other.remaining.get());
    this.committed = new LongMetric(other.committed.get());
    this.freeSpaceToSpare = new LongMetric(other.freeSpaceToSpare.get());
    this.reserved = new LongMetric(other.reserved.get());

    this.capacityPerStorageType = deepCopy(other.capacityPerStorageType);
    this.usedPerStorageType = deepCopy(other.usedPerStorageType);
    this.remainingPerStorageType = deepCopy(other.remainingPerStorageType);
    this.committedPerStorageType = deepCopy(other.committedPerStorageType);
    this.freeSpaceToSparePerStorageType = deepCopy(other.freeSpaceToSparePerStorageType);
    this.reservedPerStorageType = deepCopy(other.reservedPerStorageType);
  }

  public SCMNodeStat(Map<StorageType, Long> capacityByType, Map<StorageType, Long> usedByType,
      Map<StorageType, Long> remainingByType, Map<StorageType, Long> committedByType,
      Map<StorageType, Long> spareSpaceByType, Map<StorageType, Long> reservedByType) {
    long totalCapacity = 0, totalUsed = 0, totalRemaining = 0, totalCommitted = 0,
        totalFreeSpaceToSpare = 0, totalReserved = 0;
    for (StorageType type : StorageType.values()) {
      long typeCapacity = capacityByType.getOrDefault(type, 0L);
      long typeUsed = usedByType.getOrDefault(type, 0L);
      long typeRemaining = remainingByType.getOrDefault(type, 0L);
      long typeCommitted = committedByType.getOrDefault(type, 0L);
      long typeFreeSpaceToSpare = spareSpaceByType.getOrDefault(type, 0L);
      long typeReserved = reservedByType.getOrDefault(type, 0L);
      validate(typeCapacity, typeUsed, typeRemaining);

      capacityPerStorageType.put(type, new LongMetric(typeCapacity));
      usedPerStorageType.put(type, new LongMetric(typeUsed));
      remainingPerStorageType.put(type, new LongMetric(typeRemaining));
      committedPerStorageType.put(type, new LongMetric(typeCommitted));
      freeSpaceToSparePerStorageType.put(type, new LongMetric(typeFreeSpaceToSpare));
      reservedPerStorageType.put(type, new LongMetric(typeReserved));

      totalCapacity += typeCapacity;
      totalUsed += typeUsed;
      totalRemaining += typeRemaining;
      totalCommitted += typeCommitted;
      totalFreeSpaceToSpare += typeFreeSpaceToSpare;
      totalReserved += typeReserved;
    }

    this.capacity = new LongMetric(totalCapacity);
    this.scmUsed = new LongMetric(totalUsed);
    this.remaining = new LongMetric(totalRemaining);
    this.committed = new LongMetric(totalCommitted);
    this.freeSpaceToSpare = new LongMetric(totalFreeSpaceToSpare);
    this.reserved = new LongMetric(totalReserved);
  }

  private void validate(long newCapacity, long newUsed, long newRemaining) {
    Preconditions.checkArgument(newCapacity >= 0, "Capacity cannot be negative.");
    Preconditions.checkArgument(newUsed >= 0, "Used space cannot be negative.");
    Preconditions.checkArgument(newRemaining >= 0, "Remaining cannot be negative.");
  }

  /**
   * @return the total configured capacity of the node.
   */
  @Override
  public LongMetric getCapacity() {
    return capacity;
  }

  /**
   * @return the total SCM used space on the node.
   */
  @Override
  public LongMetric getScmUsed() {
    return scmUsed;
  }

  /**
   * @return the total remaining space available on the node.
   */
  @Override
  public LongMetric getRemaining() {
    return remaining;
  }

  /**
   *
   * @return the total committed space on the node
   */
  @Override
  public LongMetric getCommitted() {
    return committed;
  }

  /**
   * Get a min space available to spare on the node.
   * @return a min free space available to spare on the node
   */
  @Override
  public LongMetric getFreeSpaceToSpare() {
    return freeSpaceToSpare;
  }

  /**
   * Get the reserved space on the node.
   * @return the reserved space on the node
   */
  @Override
  public LongMetric getReserved() {
    return reserved;
  }

  @Override
  public LongMetric getCapacity(StorageType storageType) {
    if (storageType == null) {
      return getCapacity();
    } else {
      return capacityPerStorageType.get(storageType);
    }
  }

  @Override
  public LongMetric getScmUsed(StorageType storageType) {
    if (storageType == null) {
      return getScmUsed();
    } else {
      return usedPerStorageType.get(storageType);
    }
  }

  @Override
  public LongMetric getRemaining(StorageType storageType) {
    if (storageType == null) {
      return getRemaining();
    } else {
      return remainingPerStorageType.get(storageType);
    }
  }

  @Override
  public LongMetric getCommitted(StorageType storageType) {
    if (storageType == null) {
      return getCommitted();
    } else {
      return committedPerStorageType.get(storageType);
    }
  }

  @Override
  public LongMetric getFreeSpaceToSpare(StorageType storageType) {
    if (storageType == null) {
      return getFreeSpaceToSpare();
    } else {
      return freeSpaceToSparePerStorageType.get(storageType);
    }
  }

  @Override
  public LongMetric getReserved(StorageType storageType) {
    if (storageType == null) {
      return getReserved();
    } else {
      return reservedPerStorageType.get(storageType);
    }
  }

  /**
   * Copy the total and per-StorageType values from another stat.
   *
   * @param stat the stat whose values are copied into this one.
   */
  @Override
  @VisibleForTesting
  public void set(NodeStat stat) {
    long newCapacity = stat.getCapacity().get();
    long newUsed = stat.getScmUsed().get();
    long newRemaining = stat.getRemaining().get();
    long newCommitted = stat.getCommitted().get();
    long newFreeSpaceToSpare = stat.getFreeSpaceToSpare().get();
    long newReserved = stat.getReserved().get();
    validate(newCapacity, newUsed, newRemaining);

    this.capacity = new LongMetric(newCapacity);
    this.scmUsed = new LongMetric(newUsed);
    this.remaining = new LongMetric(newRemaining);
    this.committed = new LongMetric(newCommitted);
    this.freeSpaceToSpare = new LongMetric(newFreeSpaceToSpare);
    this.reserved = new LongMetric(newReserved);

    capacityPerStorageType.clear();
    usedPerStorageType.clear();
    remainingPerStorageType.clear();
    committedPerStorageType.clear();
    freeSpaceToSparePerStorageType.clear();
    reservedPerStorageType.clear();
    for (StorageType storageType : StorageType.values()) {
      capacityPerStorageType.put(storageType, new LongMetric(stat.getCapacity(storageType).get()));
      usedPerStorageType.put(storageType, new LongMetric(stat.getScmUsed(storageType).get()));
      remainingPerStorageType.put(storageType, new LongMetric(stat.getRemaining(storageType).get()));
      committedPerStorageType.put(storageType, new LongMetric(stat.getCommitted(storageType).get()));
      freeSpaceToSparePerStorageType.put(storageType, new LongMetric(stat.getFreeSpaceToSpare(storageType).get()));
      reservedPerStorageType.put(storageType, new LongMetric(stat.getReserved(storageType).get()));
    }
  }

  /**
   * Adds a new nodestat to existing values of the node.
   *
   * @param stat Nodestat.
   * @return SCMNodeStat
   */
  @Override
  public SCMNodeStat add(NodeStat stat) {
    long totalCapacity = 0, totalUsed = 0, totalRemaining = 0, totalCommitted = 0,
        totalFreeSpaceToSpare = 0, totalReserved = 0;
    for (StorageType storageType : StorageType.values()) {
      capacityPerStorageType.get(storageType).add(stat.getCapacity(storageType).get());
      usedPerStorageType.get(storageType).add(stat.getScmUsed(storageType).get());
      remainingPerStorageType.get(storageType).add(stat.getRemaining(storageType).get());
      committedPerStorageType.get(storageType).add(stat.getCommitted(storageType).get());
      freeSpaceToSparePerStorageType.get(storageType).add(stat.getFreeSpaceToSpare(storageType).get());
      reservedPerStorageType.get(storageType).add(stat.getReserved(storageType).get());

      totalCapacity += capacityPerStorageType.get(storageType).get();
      totalUsed += usedPerStorageType.get(storageType).get();
      totalRemaining += remainingPerStorageType.get(storageType).get();
      totalCommitted += committedPerStorageType.get(storageType).get();
      totalFreeSpaceToSpare += freeSpaceToSparePerStorageType.get(storageType).get();
      totalReserved += reservedPerStorageType.get(storageType).get();
    }

    return setTotals(totalCapacity, totalUsed, totalRemaining, totalCommitted,
        totalFreeSpaceToSpare, totalReserved);
  }

  @Override
  public SCMNodeStat add(long addCapacity, long addUsed, long addRemaining, long addCommitted,
      long addFreeSpaceToSpare, long addReserved, @Nonnull StorageType storageType) {
    capacityPerStorageType.get(storageType).add(addCapacity);
    usedPerStorageType.get(storageType).add(addUsed);
    remainingPerStorageType.get(storageType).add(addRemaining);
    committedPerStorageType.get(storageType).add(addCommitted);
    freeSpaceToSparePerStorageType.get(storageType).add(addFreeSpaceToSpare);
    reservedPerStorageType.get(storageType).add(addReserved);
    this.capacity.add(addCapacity);
    this.scmUsed.add(addUsed);
    this.remaining.add(addRemaining);
    this.committed.add(addCommitted);
    this.freeSpaceToSpare.add(addFreeSpaceToSpare);
    this.reserved.add(addReserved);
    return this;
  }

  /**
   * Subtracts the stat values from the existing NodeStat.
   *
   * @param stat SCMNodeStat.
   * @return Modified SCMNodeStat
   */
  @Override
  public SCMNodeStat subtract(NodeStat stat) {
    long totalCapacity = 0, totalUsed = 0, totalRemaining = 0, totalCommitted = 0,
        totalFreeSpaceToSpare = 0, totalReserved = 0;
    for (StorageType storageType : StorageType.values()) {
      capacityPerStorageType.get(storageType).subtract(stat.getCapacity(storageType).get());
      usedPerStorageType.get(storageType).subtract(stat.getScmUsed(storageType).get());
      remainingPerStorageType.get(storageType).subtract(stat.getRemaining(storageType).get());
      committedPerStorageType.get(storageType).subtract(stat.getCommitted(storageType).get());
      freeSpaceToSparePerStorageType.get(storageType).subtract(stat.getFreeSpaceToSpare(storageType).get());
      reservedPerStorageType.get(storageType).subtract(stat.getReserved(storageType).get());

      totalCapacity += capacityPerStorageType.get(storageType).get();
      totalUsed += usedPerStorageType.get(storageType).get();
      totalRemaining += remainingPerStorageType.get(storageType).get();
      totalCommitted += committedPerStorageType.get(storageType).get();
      totalFreeSpaceToSpare += freeSpaceToSparePerStorageType.get(storageType).get();
      totalReserved += reservedPerStorageType.get(storageType).get();
    }

    return setTotals(totalCapacity, totalUsed, totalRemaining, totalCommitted,
        totalFreeSpaceToSpare, totalReserved);
  }

  private SCMNodeStat setTotals(long totalCapacity, long totalUsed, long totalRemaining,
      long totalCommitted, long totalFreeSpaceToSpare, long totalReserved) {
    this.capacity = new LongMetric(totalCapacity);
    this.scmUsed = new LongMetric(totalUsed);
    this.remaining = new LongMetric(totalRemaining);
    this.committed = new LongMetric(totalCommitted);
    this.freeSpaceToSpare = new LongMetric(totalFreeSpaceToSpare);
    this.reserved = new LongMetric(totalReserved);
    return this;
  }

  private static Map<StorageType, LongMetric> deepCopy(Map<StorageType, LongMetric> source) {
    Map<StorageType, LongMetric> copied = new EnumMap<>(StorageType.class);
    source.forEach((key, value) -> copied.put(key, new LongMetric(value.get())));
    return copied;
  }

  @Override
  public boolean equals(Object to) {
    if (this == to) {
      return true;
    }
    if (to instanceof SCMNodeStat) {
      SCMNodeStat tempStat = (SCMNodeStat) to;
      return capacity.isEqual(tempStat.getCapacity().get()) &&
          scmUsed.isEqual(tempStat.getScmUsed().get()) &&
          remaining.isEqual(tempStat.getRemaining().get()) &&
          committed.isEqual(tempStat.getCommitted().get()) &&
          freeSpaceToSpare.isEqual(tempStat.freeSpaceToSpare.get()) &&
          reserved.isEqual(tempStat.reserved.get()) &&
          capacityPerStorageType.equals(tempStat.capacityPerStorageType) &&
          usedPerStorageType.equals(tempStat.usedPerStorageType) &&
          remainingPerStorageType.equals(tempStat.remainingPerStorageType) &&
          committedPerStorageType.equals(tempStat.committedPerStorageType) &&
          freeSpaceToSparePerStorageType.equals(tempStat.freeSpaceToSparePerStorageType) &&
          reservedPerStorageType.equals(tempStat.reservedPerStorageType);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(capacity.get() ^ scmUsed.get() ^ remaining.get() ^
        committed.get() ^ freeSpaceToSpare.get() ^ reserved.get());
    result = 31 * result + capacityPerStorageType.hashCode();
    result = 31 * result + usedPerStorageType.hashCode();
    result = 31 * result + remainingPerStorageType.hashCode();
    result = 31 * result + committedPerStorageType.hashCode();
    result = 31 * result + freeSpaceToSparePerStorageType.hashCode();
    result = 31 * result + reservedPerStorageType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SCMNodeStat{" +
        "capacity=" + capacity.get() +
        ", scmUsed=" + scmUsed.get() +
        ", remaining=" + remaining.get() +
        ", committed=" + committed.get() +
        ", freeSpaceToSpare=" + freeSpaceToSpare.get() +
        ", reserved=" + reserved.get() +
        '}';
  }
}
