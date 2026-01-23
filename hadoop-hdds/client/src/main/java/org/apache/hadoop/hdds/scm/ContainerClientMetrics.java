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

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.util.MetricUtil;

/**
 * Container client metrics that describe how data writes are distributed to
 * pipelines.
 */
@Metrics(about = "Client Metrics", context = OzoneConsts.OZONE)
public final class ContainerClientMetrics {
  private static ContainerClientMetrics instance;
  @VisibleForTesting
  static int referenceCount = 0;

  private static final String SOURCE_NAME =
      ContainerClientMetrics.class.getSimpleName();
  private static int instanceCount = 0;

  @Metric
  private MutableCounterLong totalWriteChunkCalls;
  @Metric
  private MutableCounterLong totalWriteChunkBytes;

  @Metric
  private MutableRate hsyncSynchronizedWorkNs;
  @Metric
  private MutableRate hsyncSendWriteChunkNs;
  @Metric
  private MutableRate hsyncWaitForFlushNs;
  @Metric
  private MutableRate hsyncWatchForCommitNs;
  @Metric
  private MutableCounterLong writeChunksDuringWrite;
  @Metric
  private MutableCounterLong flushesDuringWrite;

  private MutableQuantiles[] listBlockLatency;
  private MutableQuantiles[] getBlockLatency;
  private MutableQuantiles[] getCommittedBlockLengthLatency;
  private MutableQuantiles[] readChunkLatency;
  private MutableQuantiles[] getSmallFileLatency;
  private MutableQuantiles[] hsyncLatencyNs;
  private MutableQuantiles[] omHsyncLatencyNs;
  private MutableQuantiles[] datanodeHsyncLatencyNs;
  private final Map<PipelineID, MutableCounterLong> writeChunkCallsByPipeline;
  private final Map<PipelineID, MutableCounterLong> writeChunkBytesByPipeline;
  private final Map<DatanodeID, MutableCounterLong> writeChunksCallsByLeaders;
  private final MetricsRegistry registry;

  public static synchronized ContainerClientMetrics acquire() {
    if (instance == null) {
      instanceCount++;
      instance = DefaultMetricsSystem.instance().register(
          SOURCE_NAME + instanceCount,
          "Ozone Client Metrics", new ContainerClientMetrics());
    }
    referenceCount++;
    return instance;
  }

  public static synchronized void release() {
    if (instance == null) {
      throw new IllegalStateException("This metrics class is not used.");
    }
    referenceCount--;
    if (referenceCount == 0) {
      instance.stop();
      DefaultMetricsSystem.instance().unregisterSource(
          SOURCE_NAME + instanceCount);
      instance = null;
    }
  }

  private ContainerClientMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
    writeChunkCallsByPipeline = new ConcurrentHashMap<>();
    writeChunkBytesByPipeline = new ConcurrentHashMap<>();
    writeChunksCallsByLeaders = new ConcurrentHashMap<>();

    listBlockLatency = new MutableQuantiles[3];
    getBlockLatency = new MutableQuantiles[3];
    getCommittedBlockLengthLatency = new MutableQuantiles[3];
    readChunkLatency = new MutableQuantiles[3];
    getSmallFileLatency = new MutableQuantiles[3];
    hsyncLatencyNs = new MutableQuantiles[3];
    omHsyncLatencyNs = new MutableQuantiles[3];
    datanodeHsyncLatencyNs = new MutableQuantiles[3];
    int[] intervals = {60, 300, 900};
    for (int i = 0; i < intervals.length; i++) {
      int interval = intervals[i];
      listBlockLatency[i] = registry
          .newQuantiles("listBlockLatency" + interval
                  + "s", "ListBlock latency in microseconds", "ops",
              "latency", interval);
      getBlockLatency[i] = registry
          .newQuantiles("getBlockLatency" + interval
                  + "s", "GetBlock latency in microseconds", "ops",
              "latency", interval);
      getCommittedBlockLengthLatency[i] = registry
          .newQuantiles("getCommittedBlockLengthLatency" + interval
                  + "s", "GetCommittedBlockLength latency in microseconds",
              "ops", "latency", interval);
      readChunkLatency[i] = registry
          .newQuantiles("readChunkLatency" + interval
                  + "s", "ReadChunk latency in microseconds", "ops",
              "latency", interval);
      getSmallFileLatency[i] = registry
          .newQuantiles("getSmallFileLatency" + interval
                  + "s", "GetSmallFile latency in microseconds", "ops",
              "latency", interval);
      hsyncLatencyNs[i] = registry
          .newQuantiles("hsyncLatency" + interval
                  + "s", "client hsync latency in nanoseconds", "ops",
              "latency", interval);
      omHsyncLatencyNs[i] = registry
          .newQuantiles("omHsyncLatency" + interval
                  + "s", "client hsync latency to OM in nanoseconds", "ops",
              "latency", interval);
      datanodeHsyncLatencyNs[i] = registry
          .newQuantiles("dnHsyncLatency" + interval
                  + "s", "client hsync latency to DN in nanoseconds", "ops",
              "latency", interval);
    }
  }

  public void stop() {
    MetricUtil.stop(listBlockLatency);
    MetricUtil.stop(getBlockLatency);
    MetricUtil.stop(getCommittedBlockLengthLatency);
    MetricUtil.stop(readChunkLatency);
    MetricUtil.stop(getSmallFileLatency);
    MetricUtil.stop(hsyncLatencyNs);
    MetricUtil.stop(omHsyncLatencyNs);
    MetricUtil.stop(datanodeHsyncLatencyNs);
  }

  public void recordWriteChunk(Pipeline pipeline, long chunkSizeBytes) {
    writeChunkCallsByPipeline.computeIfAbsent(pipeline.getId(),
        pipelineID -> registry.newCounter(
            Interns.info("writeChunkCallsPipeline-" + pipelineID.getId(),
                "Number of writeChunk calls on a pipelines"),
            0L)
    ).incr();
    writeChunkBytesByPipeline.computeIfAbsent(pipeline.getId(),
        pipelineID -> registry.newCounter(
            Interns.info("writeChunkBytesPipeline-" + pipelineID.getId(),
                "Number of bytes written on a pipelines"),
            0L)
    ).incr(chunkSizeBytes);
    if (pipeline.getLeaderId() != null) {
      writeChunksCallsByLeaders.computeIfAbsent(pipeline.getLeaderId(),
          leader -> registry.newCounter(
              Interns.info("writeChunkCallsLeader-" + leader,
                  "Number of writeChunk calls on a leader node"),
              0L)
      ).incr();
    }
    totalWriteChunkCalls.incr();
    totalWriteChunkBytes.incr(chunkSizeBytes);
  }

  public void addListBlockLatency(long latency) {
    for (MutableQuantiles q : listBlockLatency) {
      if (q != null) {
        q.add(latency);
      }
    }
  }

  public void addHsyncLatency(long hsyncLatencyTime) {
    for (MutableQuantiles q : hsyncLatencyNs) {
      if (q != null) {
        q.add(hsyncLatencyTime);
      }
    }
  }

  public void addGetBlockLatency(long latency) {
    for (MutableQuantiles q : getBlockLatency) {
      if (q != null) {
        q.add(latency);
      }
    }
  }

  public void addOMHsyncLatency(long hsyncLatencyTime) {
    for (MutableQuantiles q : omHsyncLatencyNs) {
      if (q != null) {
        q.add(hsyncLatencyTime);
      }
    }
  }

  public void addGetCommittedBlockLengthLatency(long latency) {
    for (MutableQuantiles q : getCommittedBlockLengthLatency) {
      if (q != null) {
        q.add(latency);
      }
    }
  }

  public void addReadChunkLatency(long latency) {
    for (MutableQuantiles q : readChunkLatency) {
      if (q != null) {
        q.add(latency);
      }
    }
  }

  public void addGetSmallFileLatency(long latency) {
    for (MutableQuantiles q : getSmallFileLatency) {
      if (q != null) {
        q.add(latency);
      }
    }
  }

  public void addDataNodeHsyncLatency(long hsyncLatencyTime) {
    for (MutableQuantiles q : datanodeHsyncLatencyNs) {
      if (q != null) {
        q.add(hsyncLatencyTime);
      }
    }
  }

  @VisibleForTesting
  public MutableCounterLong getTotalWriteChunkBytes() {
    return totalWriteChunkBytes;
  }

  MutableCounterLong getTotalWriteChunkCalls() {
    return totalWriteChunkCalls;
  }

  Map<PipelineID, MutableCounterLong> getWriteChunkBytesByPipeline() {
    return writeChunkBytesByPipeline;
  }

  Map<PipelineID, MutableCounterLong> getWriteChunkCallsByPipeline() {
    return writeChunkCallsByPipeline;
  }

  Map<DatanodeID, MutableCounterLong> getWriteChunksCallsByLeaders() {
    return writeChunksCallsByLeaders;
  }

  public MutableRate getHsyncSynchronizedWorkNs() {
    return hsyncSynchronizedWorkNs;
  }

  public MutableRate getHsyncSendWriteChunkNs() {
    return hsyncSendWriteChunkNs;
  }

  public MutableRate getHsyncWaitForFlushNs() {
    return hsyncWaitForFlushNs;
  }

  public MutableRate getHsyncWatchForCommitNs() {
    return hsyncWatchForCommitNs;
  }

  public MutableCounterLong getWriteChunksDuringWrite() {
    return writeChunksDuringWrite;
  }

  public MutableCounterLong getFlushesDuringWrite() {
    return flushesDuringWrite;
  }
}
