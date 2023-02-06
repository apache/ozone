/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
  private final Map<PipelineID, MutableCounterLong> writeChunkCallsByPipeline;
  private final Map<PipelineID, MutableCounterLong> writeChunkBytesByPipeline;
  private final Map<UUID, MutableCounterLong> writeChunksCallsByLeaders;
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

  @VisibleForTesting
  public MutableCounterLong getTotalWriteChunkBytes() {
    return totalWriteChunkBytes;
  }

  @VisibleForTesting
  public MutableCounterLong getTotalWriteChunkCalls() {
    return totalWriteChunkCalls;
  }

  @VisibleForTesting
  public Map<PipelineID, MutableCounterLong> getWriteChunkBytesByPipeline() {
    return writeChunkBytesByPipeline;
  }

  @VisibleForTesting
  public Map<PipelineID, MutableCounterLong> getWriteChunkCallsByPipeline() {
    return writeChunkCallsByPipeline;
  }

  @VisibleForTesting
  public Map<UUID, MutableCounterLong> getWriteChunksCallsByLeaders() {
    return writeChunksCallsByLeaders;
  }
}
