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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportBase;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor.IQueueMetrics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Customized queue to handle FCR and ICR from datanode optimally,
 * avoiding duplicate FCR reports.
 */
public class ContainerReportQueue<VALUE extends ContainerReportBase>
    implements BlockingQueue<VALUE>, IQueueMetrics {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerReportQueue.class);

  private static final Integer MAX_CAPACITY = 100000;

  private LinkedBlockingQueue<String> orderingQueue
      = new LinkedBlockingQueue<>();
  private Map<String, List<VALUE>> dataMap = new HashMap<>();

  private int reportCount = 0;

  private AtomicInteger droppedCount = new AtomicInteger();

  private boolean addContainerReport(VALUE val) {
    ContainerReportFromDatanode report
        = (ContainerReportFromDatanode) val;
    synchronized (this) {
      // 1. check if no previous report available, else add the report
      DatanodeDetails dn = report.getDatanodeDetails();
      if (!dataMap.containsKey(dn.getUuidString())) {
        ArrayList<VALUE> dataList = new ArrayList<>();
        dataList.add(val);
        ++reportCount;
        dataMap.put(dn.getUuidString(), dataList);
        orderingQueue.add(dn.getUuidString());
        return true;
      }

      // 2. FCR report available
      List<VALUE> dataList = dataMap.get(dn.getUuidString());
      boolean isReportRemoved = false;
      if (!dataList.isEmpty()) {
        // remove FCR and ICR (filter out deleted Container Report)
        for (int i = dataList.size() - 1; i >= 0; --i) {
          ContainerReportBase reportInfo = dataList.get(i);
          // if FCR, its last FCR report, remove directly
          if (isFCRReport(reportInfo)) {
            dataList.remove(i);
            --reportCount;
            droppedCount.incrementAndGet();
            isReportRemoved = true;
            break;
          }
        }
      }

      dataList.add(val);
      ++reportCount;
      if (!isReportRemoved) {
        orderingQueue.add(dn.getUuidString());
      }
    }
    return true;
  }

  private boolean addIncrementalReport(VALUE val) {
    IncrementalContainerReportFromDatanode report
        = (IncrementalContainerReportFromDatanode) val;
    synchronized (this) {
      // 1. check if no previous report available, else add the report
      DatanodeDetails dn = report.getDatanodeDetails();
      if (!dataMap.containsKey(dn.getUuidString())) {
        ArrayList<VALUE> dataList = new ArrayList<>();
        dataList.add(val);
        ++reportCount;
        dataMap.put(dn.getUuidString(), dataList);
        orderingQueue.add(dn.getUuidString());
        return true;
      }

      // 2. Add ICR report or merge to previous ICR
      List<VALUE> dataList = dataMap.get(dn.getUuidString());
      dataList.add(val);
      ++reportCount;
      orderingQueue.add(dn.getUuidString());
    }
    return true;
  }

  private boolean isFCRReport(Object report) {
    return report instanceof ContainerReportFromDatanode;
  }

  private boolean isICRReport(Object report) {
    return report instanceof IncrementalContainerReportFromDatanode;
  }

  private VALUE removeAndGet(String uuid) {
    if (uuid == null) {
      return null;
    }

    List<VALUE> dataList = dataMap.get(uuid);
    VALUE report = null;
    if (dataList != null && !dataList.isEmpty()) {
      report = dataList.remove(0);
      --reportCount;
      if (dataList.isEmpty()) {
        dataMap.remove(uuid);
      }
    }
    return report;
  }

  private VALUE getReport(String uuid) {
    if (uuid == null) {
      return null;
    }

    List<VALUE> dataList = dataMap.get(uuid);
    if (dataList != null && !dataList.isEmpty()) {
      return dataList.get(0);
    }
    return null;
  }

  @Override
  public boolean add(@NotNull VALUE value) {
    synchronized (this) {
      if (remainingCapacity() == 0) {
        return false;
      }

      if (isFCRReport(value)) {
        return addContainerReport(value);
      } else if (isICRReport(value)) {
        return addIncrementalReport(value);
      }
      return false;
    }
  }

  @Override
  public boolean offer(@NotNull VALUE value) {
    synchronized (this) {
      return add(value);
    }
  }

  @Override
  public VALUE remove() {
    synchronized (this) {
      String uuid = orderingQueue.remove();
      return removeAndGet(uuid);
    }
  }

  @Override
  public VALUE poll() {
    synchronized (this) {
      String uuid = orderingQueue.poll();
      return removeAndGet(uuid);
    }
  }

  @Override
  public VALUE element() {
    synchronized (this) {
      String uuid = orderingQueue.element();
      return getReport(uuid);
    }
  }

  @Override
  public VALUE peek() {
    synchronized (this) {
      String uuid = orderingQueue.peek();
      return getReport(uuid);
    }
  }

  @Override
  public void put(@NotNull VALUE value) throws InterruptedException {
    while (!add(value)) {
      Thread.currentThread().sleep(10);
    }
  }

  @Override
  public boolean offer(VALUE value, long timeout, @NotNull TimeUnit unit)
      throws InterruptedException {
    long timeoutMillis = unit.toMillis(timeout);
    while (timeoutMillis > 0) {
      if (add(value)) {
        return true;
      }
      Thread.currentThread().sleep(10);
      timeoutMillis -= 10;
    }
    return false;
  }

  @NotNull
  @Override
  public VALUE take() throws InterruptedException {
    String uuid = orderingQueue.take();
    synchronized (this) {
      return removeAndGet(uuid);
    }
  }

  @Nullable
  @Override
  public VALUE poll(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException {
    String uuid = orderingQueue.poll(timeout, unit);
    synchronized (this) {
      return removeAndGet(uuid);
    }
  }

  @Override
  public int remainingCapacity() {
    synchronized (this) {
      return MAX_CAPACITY - reportCount;
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
  public boolean addAll(@NotNull Collection<? extends VALUE> c) {
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
      reportCount = 0;
    }
  }

  @Override
  public int size() {
    synchronized (this) {
      return reportCount;
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
  public Iterator<VALUE> iterator() {
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
  public int drainTo(@NotNull Collection<? super VALUE> c) {
    // no need support this
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public int drainTo(@NotNull Collection<? super VALUE> c, int maxElements) {
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
