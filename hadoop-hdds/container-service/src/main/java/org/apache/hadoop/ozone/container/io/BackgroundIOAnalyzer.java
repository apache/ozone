/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.io;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class BackgroundIOAnalyzer extends Thread {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundIOAnalyzer.class);

  private static final String NAME = "BackgroundIOAnalyzer";
  private static final String PROCFS_STAT = "/proc/stat";
  private static final String PROCFS_CPU = "cpu";
  private DataNodeIOMetrics metrics;
  private final AtomicBoolean stopping;
  private final long remainingSleep;

  public BackgroundIOAnalyzer(IOAnalyzerConfiguration conf) {
    this.metrics = DataNodeIOMetrics.create();
    this.stopping = new AtomicBoolean(false);
    this.remainingSleep = conf.getIOAnalyzerInterval();
    setName(NAME);
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (!stopping.get()) {
        analyzerIoWaitAndSystem();
      }
      LOG.info("{} exiting.", this);
    } catch (Exception e) {
      LOG.error("{} exiting because of exception ", this, e);
    } finally {
      if (metrics != null) {
        metrics.unregister();
      }
    }
  }

  /**
   * Analyzes the usage of IOWait and System metrics.
   *
   * <h2>Drive Types</h2>
   * <ul>
   *   <li><strong>HDDs and SSDs</strong>: May experience spikes in IOWait during read/write operations.</li>
   *   <li><strong>NVMe drives</strong>: Can result in high System values during intensive read/write activities.</li>
   * </ul>
   *
   * <h2>Monitoring Purpose</h2>
   * <p>Monitoring IOWait and System metrics is crucial for:</p>
   * <ul>
   *   <li>Identifying performance bottlenecks.</li>
   *   <li>Guiding future management decisions.</li>
   * </ul>
   *
   * <h2>Data Collection Approach</h2>
   * <p>This method employs a lightweight strategy to gather relevant data by:</p>
   * <ul>
   *   <li>Reading the <code>/proc/stat</code> file to obtain current CPU usage.</li>
   *   <li>Focusing solely on the first line of this file for metrics.</li>
   * </ul>
   *
   * <h2>Metrics Breakdown</h2>
   * <table border="1">
   *   <tr>
   *     <th>user</th>
   *     <th>nice</th>
   *     <th>system</th>
   *     <th>idle</th>
   *     <th>iowait</th>
   *     <th>irq</th>
   *     <th>softirq</th>
   *     <th>steal</th>
   *     <th>guest</th>
   *     <th>guest_nice</th>
   *   </tr>
   *   <tr>
   *     <td>10672634648</td>
   *     <td>17921665</td>
   *     <td>1413355479</td>
   *     <td>397261436142</td>
   *     <td>17949886</td>
   *     <td>0</td>
   *     <td>55070272</td>
   *     <td>0</td>
   *     <td>0</td>
   *     <td>0</td>
   *   </tr>
   * </table>
   *
   */
  private void analyzerIoWaitAndSystem() {

    if (!Shell.LINUX) {
      LOG.warn("Analyzing IO: We currently only support Linux systems.");
      return;
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(PROCFS_STAT))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(PROCFS_CPU)) {
          String[] values = line.split("\\s+");
          if(ArrayUtils.isNotEmpty(values)) {

            // Step1. Retrieve all CPU system time data.
            long user = Long.parseLong(values[1]);
            long nice = Long.parseLong(values[2]);
            long system = Long.parseLong(values[3]);
            long idle = Long.parseLong(values[4]);
            long iowait = Long.parseLong(values[5]);

            // Step2. Calculate total CPU time.
            long totalCpuTime = user + nice + system + idle + iowait;

            // Step3. Calculate the ratio.
            long iowaitRatio = (long) Math.floor((double) iowait / totalCpuTime * 100);
            metrics.setDNIoWait(iowaitRatio);

            long systemRatio = (long) Math.floor((double) system / totalCpuTime * 100);
            metrics.setDNSystem(systemRatio);

            LOG.debug("IO Analyzer : IoWait = {}, System = {}.", iowaitRatio, systemRatio);
          }
          break;
        }
      }
    } catch (IOException e) {
      LOG.error("An error occurred during the Analyzing IO process.", e);
    }

    // We collect IO performance data at regular intervals,
    // which is usually every 30 seconds.
    handleRemainingSleep(remainingSleep);
  }

  public final void handleRemainingSleep(long remainingSleep) {
    if (remainingSleep > 0) {
      try {
        Thread.sleep(remainingSleep);
      } catch (InterruptedException ignored) {
        stopping.set(true);
        LOG.warn("Background IOAnalyzer was interrupted.");
        Thread.currentThread().interrupt();
      }
    }
  }

  public synchronized void shutdown() {
    if (stopping.compareAndSet(false, true)) {
      this.interrupt();
      try {
        this.join();
      } catch (InterruptedException ex) {
        LOG.warn("Unexpected exception while stopping io analyzer.", ex);
        Thread.currentThread().interrupt();
      }
    }

    if (metrics != null) {
      metrics.unregister();
    }
  }
}
