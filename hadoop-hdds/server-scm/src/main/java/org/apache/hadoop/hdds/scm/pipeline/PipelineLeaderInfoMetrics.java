/**
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

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * This class maintains Pipeline Leader related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "Pipeline Leader-Info Metrics", context = OzoneConsts.OZONE)
public final class PipelineLeaderInfoMetrics {

  private static final String SOURCE_NAME =
      PipelineLeaderInfoMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static PipelineLeaderInfoMetrics instance;
  private PipelineManagerImpl pipelineManager;


  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineLeaderInfoMetrics.class);

  public PipelineLeaderInfoMetrics(PipelineManagerImpl ref) {
    this.registry = new MetricsRegistry(SOURCE_NAME);
    this.pipelineManager = ref;
    init();
  }

  /**
   * Register the metrics instance.
   */
  public void init() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(SOURCE_NAME, "PipeLine Leaders list", this);
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric("Returns all pipeline with their associated leader node")
  public String getPipelineLeaders() {
    List<Pipeline> pipelines = pipelineManager.getPipelines();
    StringBuilder sb = new StringBuilder();
    pipelines.forEach(pipeline -> {
      String leaderNode = "";
      UUID pipelineId = pipeline.getId().getId();

      try {
        leaderNode = pipeline.getLeaderNode().getHostName();
      } catch (IOException ioEx) {
        LOG.warn("Cannot get leader node for pipeline {}",
            pipelineId, ioEx);
      }
      sb.append(
          String.format(
              " { PipeLine-Id: %s | Leader-Id: %s } ",
              pipelineId.toString(),
              leaderNode
          ));
    });
    return String.valueOf(sb);
  }

}
