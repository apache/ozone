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
package org.apache.hadoop.hdds.scm.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor.IQueueMetrics;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Customized queue to handle FCR and ICR from datanode optimally,
 * avoiding duplicate FCR reports.
 */
public class ContainerReportQueue
    implements BlockingQueue<ContainerReport>, IQueueMetrics {

  private final Integer maxCapacity;

  /* ordering queue provides ordering of execution in fair manner
   * i.e. report execution from multiple datanode will be executed in same
   * order as added to queue.
   */
  private LinkedBlockingQueue<String> orderingQueue
      = new LinkedBlockingQueue<>();
  private Map<String, List<ContainerReport>> dataMap = new HashMap<>();

  private int capacity = 0;

  private AtomicInteger droppedCount = new AtomicInteger();

  public ContainerReportQueue() {
    this(100000);
  }
  
  public ContainerReportQueue(int maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  private boolean addContainerReport(ContainerReport val) {
    String uuidString = val.getDatanodeDetails().getUuidString();
    synchronized (this) {
      // 1. check if no previous report available, else add the report
      if (!dataMap.containsKey(uuidString)) {
        addReport(val, uuidString);
        return true;
      }

      // 2. FCR report available
      List<ContainerReport> dataList = dataMap.get(uuidString);
      boolean isReportRemoved = false;
      if (!dataList.isEmpty()) {
        // remove FCR if present
        for (int i = dataList.size() - 1; i >= 0; --i) {
          ContainerReport reportInfo = dataList.get(i);
          // if FCR, its last FCR report, remove directly
          if (SCMDatanodeHeartbeatDispatcher.ContainerReportType.FCR
              == reportInfo.getType()) {
            dataList.remove(i);
            --capacity;
            droppedCount.incrementAndGet();
            isReportRemoved = true;
            break;
          }
        }
      }

      dataList.add(val);
      ++capacity;
      if (!isReportRemoved) {
        orderingQueue.add(uuidString);
      }
    }
    return true;
  }

  private boolean addIncrementalReport(ContainerReport val) {
    String uuidString = val.getDatanodeDetails().getUuidString();
    synchronized (this) {
      // 1. check if no previous report available, else add the report
      if (!dataMap.containsKey(uuidString)) {
        addReport(val, uuidString);
        return true;
      }

      // 2. Add ICR report or merge to previous ICR
      List<ContainerReport> dataList = dataMap.get(uuidString);
      dataList.add(val);
      ++capacity;
      orderingQueue.add(uuidString);
    }
    return true;
  }

  private void addReport(ContainerReport val, String uuidString) {
    ArrayList<ContainerReport> dataList = new ArrayList<>();
    dataList.add(val);
    ++capacity;
    dataMap.put(uuidString, dataList);
    orderingQueue.add(uuidString);
  }

  private ContainerReport removeAndGet(String uuid) {
    if (uuid == null) {
      return null;
    }

    List<ContainerReport> dataList = dataMap.get(uuid);
    ContainerReport report = null;
    if (dataList != null && !dataList.isEmpty()) {
      report = dataList.remove(0);
      --capacity;
      if (dataList.isEmpty()) {
        dataMap.remove(uuid);
      }
    }
    return report;
  }

  private ContainerReport getReport(String uuid) {
    if (uuid == null) {
      return null;
    }

    List<ContainerReport> dataList = dataMap.get(uuid);
    if (dataList != null && !dataList.isEmpty()) {
      return dataList.get(0);
    }
    return null;
  }

  public boolean addValue(@NotNull ContainerReport value) {
    synchronized (this) {
      if (remainingCapacity() == 0) {
        return false;
      }

      if (SCMDatanodeHeartbeatDispatcher.ContainerReportType.FCR
          == value.getType()) {
        return addContainerReport(value);
      } else if (SCMDatanodeHeartbeatDispatcher.ContainerReportType.ICR
          == value.getType()) {
        return addIncrementalReport(value);
      }
      return false;
    }
  }

  @Override
  public boolean add(@NotNull ContainerReport value) {
    Objects.requireNonNull(value);
    synchronized (this) {
      if (remainingCapacity() == 0) {
        throw new IllegalStateException("capacity not available");
      }

      return addValue(value);
    }
  }

  @Override
  public boolean offer(@NotNull ContainerReport value) {
    Objects.requireNonNull(value);
    synchronized (this) {
      return addValue(value);
    }
  }

  @Override
  public ContainerReport remove() {
    synchronized (this) {
      String uuid = orderingQueue.remove();
      return removeAndGet(uuid);
    }
  }

  @Override
  public ContainerReport poll() {
    synchronized (this) {
      String uuid = orderingQueue.poll();
      return removeAndGet(uuid);
    }
  }

  @Override
  public ContainerReport element() {
    synchronized (this) {
      String uuid = orderingQueue.element();
      return getReport(uuid);
    }
  }

  @Override
  public ContainerReport peek() {
    synchronized (this) {
      String uuid = orderingQueue.peek();
      return getReport(uuid);
    }
  }

  @Override
  public void put(@NotNull ContainerReport value) throws InterruptedException {
    Objects.requireNonNull(value);
    while (!addValue(value)) {
      Thread.currentThread().sleep(10);
    }
  }

  @Override
  public boolean offer(ContainerReport value, long timeout,
                       @NotNull TimeUnit unit) throws InterruptedException {
    Objects.requireNonNull(value);
    long timeoutMillis = unit.toMillis(timeout);
    while (timeoutMillis > 0) {
      if (addValue(value)) {
        return true;
      }
      long startTime = Time.monotonicNow();
      Thread.currentThread().sleep(10);
      long timeDiff = Time.monotonicNow() - startTime;
      timeoutMillis -= timeDiff;
    }
    return false;
  }

  @NotNull
  @Override
  public ContainerReport take() throws InterruptedException {
    String uuid = orderingQueue.take();
    synchronized (this) {
      return removeAndGet(uuid);
    }
  }

  @Nullable
  @Override
  public ContainerReport poll(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException {
    String uuid = orderingQueue.poll(timeout, unit);
    synchronized (this) {
      return removeAndGet(uuid);
    }
  }

  @Override
  public int remainingCapacity() {
    synchronized (this) {
      return maxCapacity - capacity;
    }
  }

  @Override
  public boolean remove(Object o) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends ContainerReport> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void clear() {
    synchronized (this) {
      orderingQueue.clear();
      dataMap.clear();
      capacity = 0;
    }
  }

  @Override
  public int size() {
    synchronized (this) {
      return capacity;
    }
  }

  @Override
  public boolean isEmpty() {
    return orderingQueue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @NotNull
  @Override
  public Iterator<ContainerReport> iterator() {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @NotNull
  @Override
  public Object[] toArray() {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @NotNull
  @Override
  public <T> T[] toArray(@NotNull T[] a) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int drainTo(@NotNull Collection<? super ContainerReport> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int drainTo(@NotNull Collection<? super ContainerReport> c,
                     int maxElements) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int getAndResetDropCount(String type) {
    if (ContainerReportFromDatanode.class.getSimpleName().equals(type)) {
      // dropped count for only FCR report
      return droppedCount.getAndSet(0);
    }
    return 0;
  }
}
