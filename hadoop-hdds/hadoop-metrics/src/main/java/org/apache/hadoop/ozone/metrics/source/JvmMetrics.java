/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.source;

import com.google.common.base.Preconditions;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.ozone.metrics.MetricsCollector;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;
import org.apache.hadoop.ozone.metrics.MetricsSource;
import org.apache.hadoop.ozone.metrics.MetricsSystem;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ozone.metrics.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.metrics.lib.Interns;
import org.apache.hadoop.util.GcTimeMonitor;
import org.apache.hadoop.util.JvmPauseMonitor;

import static org.apache.hadoop.ozone.metrics.impl.MsInfo.ProcessName;
import static org.apache.hadoop.ozone.metrics.impl.MsInfo.SessionId;
import static org.apache.hadoop.ozone.metrics.source.JvmMetricsInfo.*;

/**
 * JVM and logging related metrics.
 * Mostly used by various servers as a part of the metrics they export.
 */
public class JvmMetrics implements MetricsSource {
  enum Singleton {
    INSTANCE;

    JvmMetrics impl;

    synchronized JvmMetrics init(String processName, String sessionId) {
      if (impl == null) {
        impl = create(processName, sessionId, DefaultMetricsSystem.instance());
      }
      return impl;
    }

    synchronized void shutdown() {
      DefaultMetricsSystem.instance().unregisterSource(JvmMetrics.name());
      impl = null;
    }
  }

  @VisibleForTesting
  public synchronized void registerIfNeeded() {
    // during tests impl might exist, but is not registered
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource("JvmMetrics") == null) {
      ms.register(JvmMetrics.name(), JvmMetrics.description(), this);
    }
  }

  static final float M = 1024*1024;
  static public final float MEMORY_MAX_UNLIMITED_MB = -1;

  final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
  final List<GarbageCollectorMXBean> gcBeans =
      ManagementFactory.getGarbageCollectorMXBeans();
  private ThreadMXBean threadMXBean;
  final String processName, sessionId;
  private JvmPauseMonitor pauseMonitor = null;
  final ConcurrentHashMap<String, MetricsInfo[]> gcInfoCache =
      new ConcurrentHashMap<String, MetricsInfo[]>();
  private GcTimeMonitor gcTimeMonitor = null;

  @VisibleForTesting
  JvmMetrics(String processName, String sessionId, boolean useThreadMXBean) {
    this.processName = processName;
    this.sessionId = sessionId;
    if (useThreadMXBean) {
      this.threadMXBean = ManagementFactory.getThreadMXBean();
    }
  }

  public void setPauseMonitor(final JvmPauseMonitor pauseMonitor) {
    this.pauseMonitor = pauseMonitor;
  }

  public void setGcTimeMonitor(GcTimeMonitor gcTimeMonitor) {
    Preconditions.checkNotNull(gcTimeMonitor);
    this.gcTimeMonitor = gcTimeMonitor;
  }

  public static JvmMetrics create(String processName, String sessionId,
                                  MetricsSystem ms) {
    // Reloading conf instead of getting from outside since it's redundant in
    // code level to update all the callers across lots of modules,
    // this method is called at most once for components (NN/DN/RM/NM/...)
    // so that the overall cost is not expensive.
    boolean useThreadMXBean = new Configuration().getBoolean(
        CommonConfigurationKeys.HADOOP_METRICS_JVM_USE_THREAD_MXBEAN,
        CommonConfigurationKeys.HADOOP_METRICS_JVM_USE_THREAD_MXBEAN_DEFAULT);
    return ms.register(JvmMetrics.name(), JvmMetrics.description(),
                       new JvmMetrics(processName, sessionId, useThreadMXBean));
  }

  public static void reattach(MetricsSystem ms, JvmMetrics jvmMetrics) {
    ms.register(JvmMetrics.name(), JvmMetrics.description(), jvmMetrics);
  }

  public static JvmMetrics initSingleton(String processName, String sessionId) {
    return Singleton.INSTANCE.init(processName, sessionId);
  }

  /**
   * Shutdown the JvmMetrics singleton. This is not necessary if the JVM itself
   * is shutdown, but may be necessary for scenarios where JvmMetrics instance
   * needs to be re-created while the JVM is still around. One such scenario
   * is unit-testing.
   */
  public static void shutdownSingleton() {
    Singleton.INSTANCE.shutdown();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(JvmMetrics)
        .setContext("jvm").tag(ProcessName, processName)
        .tag(SessionId, sessionId);
    getMemoryUsage(rb);
    getGcUsage(rb);
    if (threadMXBean != null) {
      getThreadUsage(rb);
    } else {
      getThreadUsageFromGroup(rb);
    }
  }

  private void getMemoryUsage(MetricsRecordBuilder rb) {
    MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    Runtime runtime = Runtime.getRuntime();
    rb.addGauge(MemNonHeapUsedM, memNonHeap.getUsed() / M)
      .addGauge(MemNonHeapCommittedM, memNonHeap.getCommitted() / M)
      .addGauge(MemNonHeapMaxM, calculateMaxMemoryUsage(memNonHeap))
      .addGauge(MemHeapUsedM, memHeap.getUsed() / M)
      .addGauge(MemHeapCommittedM, memHeap.getCommitted() / M)
      .addGauge(MemHeapMaxM, calculateMaxMemoryUsage(memHeap))
      .addGauge(MemMaxM, runtime.maxMemory() / M);
  }

  private float calculateMaxMemoryUsage(MemoryUsage memHeap) {
    long max =  memHeap.getMax() ;

     if (max == -1) {
       return MEMORY_MAX_UNLIMITED_MB;
     }

    return max / M;
  }

  private void getGcUsage(MetricsRecordBuilder rb) {
    long count = 0;
    long timeMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
      if (gcBean.getName() != null) {
        String name = gcBean.getName();
        // JDK-8265136 Skip concurrent phase
        if (name.startsWith("ZGC") && name.endsWith("Cycles")) {
          continue;
        }
      }
      long c = gcBean.getCollectionCount();
      long t = gcBean.getCollectionTime();
      MetricsInfo[] gcInfo = getGcInfo(gcBean.getName());
      rb.addCounter(gcInfo[0], c).addCounter(gcInfo[1], t);
      count += c;
      timeMillis += t;
    }
    rb.addCounter(GcCount, count)
      .addCounter(GcTimeMillis, timeMillis);
    
    if (pauseMonitor != null) {
      rb.addCounter(GcNumWarnThresholdExceeded,
          pauseMonitor.getNumGcWarnThresholdExceeded());
      rb.addCounter(GcNumInfoThresholdExceeded,
          pauseMonitor.getNumGcInfoThresholdExceeded());
      rb.addCounter(GcTotalExtraSleepTime,
          pauseMonitor.getTotalGcExtraSleepTime());
    }

    if (gcTimeMonitor != null) {
      rb.addGauge(GcTimePercentage,
          gcTimeMonitor.getLatestGcData().getGcTimePercentage());
    }
  }

  private MetricsInfo[] getGcInfo(String gcName) {
    MetricsInfo[] gcInfo = gcInfoCache.get(gcName);
    if (gcInfo == null) {
      gcInfo = new MetricsInfo[2];
      gcInfo[0] = Interns.info("GcCount" + gcName, "GC Count for " + gcName);
      gcInfo[1] = Interns.info("GcTimeMillis" + gcName, "GC Time for " + gcName);
      MetricsInfo[] previousGcInfo = gcInfoCache.putIfAbsent(gcName, gcInfo);
      if (previousGcInfo != null) {
        return previousGcInfo;
      }
    }
    return gcInfo;
  }

  private void getThreadUsage(MetricsRecordBuilder rb) {
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;
    long threadIds[] = threadMXBean.getAllThreadIds();
    for (ThreadInfo threadInfo : threadMXBean.getThreadInfo(threadIds, 0)) {
      if (threadInfo == null) continue; // race protection
      switch (threadInfo.getThreadState()) {
        case NEW:           threadsNew++;           break;
        case RUNNABLE:      threadsRunnable++;      break;
        case BLOCKED:       threadsBlocked++;       break;
        case WAITING:       threadsWaiting++;       break;
        case TIMED_WAITING: threadsTimedWaiting++;  break;
        case TERMINATED:    threadsTerminated++;    break;
      }
    }
    rb.addGauge(ThreadsNew, threadsNew)
      .addGauge(ThreadsRunnable, threadsRunnable)
      .addGauge(ThreadsBlocked, threadsBlocked)
      .addGauge(ThreadsWaiting, threadsWaiting)
      .addGauge(ThreadsTimedWaiting, threadsTimedWaiting)
      .addGauge(ThreadsTerminated, threadsTerminated);
  }

  private void getThreadUsageFromGroup(MetricsRecordBuilder rb) {
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;
    ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
    Thread[] threads = new Thread[threadGroup.activeCount()];
    threadGroup.enumerate(threads);
    for (Thread thread : threads) {
      if (thread == null) {
        // race protection
        continue;
      }
      switch (thread.getState()) {
      case NEW:           threadsNew++;           break;
      case RUNNABLE:      threadsRunnable++;      break;
      case BLOCKED:       threadsBlocked++;       break;
      case WAITING:       threadsWaiting++;       break;
      case TIMED_WAITING: threadsTimedWaiting++;  break;
      case TERMINATED:    threadsTerminated++;    break;
      default:
      }
    }
    rb.addGauge(ThreadsNew, threadsNew)
        .addGauge(ThreadsRunnable, threadsRunnable)
        .addGauge(ThreadsBlocked, threadsBlocked)
        .addGauge(ThreadsWaiting, threadsWaiting)
        .addGauge(ThreadsTimedWaiting, threadsTimedWaiting)
        .addGauge(ThreadsTerminated, threadsTerminated);
  }

}
