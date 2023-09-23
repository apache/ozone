/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The capacity pipeline choose policy that chooses pipeline
 * with relatively more remaining datanode space.
 */
public class CapacityPipelineChoosePolicy implements PipelineChoosePolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineChoosePolicy.class);

  private final NodeManager nodeManager;

  private PipelineChoosePolicy healthPolicy;

  public CapacityPipelineChoosePolicy(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
    healthPolicy = new HealthyPipelineChoosePolicy(nodeManager);
  }

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    Pipeline targetPipeline;
    Pipeline pipeline1 = healthPolicy.choosePipeline(pipelineList, pri);
    Pipeline pipeline2 = healthPolicy.choosePipeline(pipelineList, pri);
    if (pipeline1.getId().equals(pipeline2.getId())) {
      targetPipeline = pipeline1;
      LOG.debug("Chosen pipeline = {}", targetPipeline);
    } else {
      SCMNodeMetric metric1 = getMaxUsageNodeFromPipeline(pipeline1);
      SCMNodeMetric metric2 = getMaxUsageNodeFromPipeline(pipeline2);
      if (metric1 == null || metric2 == null) {
        LOG.warn("Can't get SCMNodeStat from pipeline: {} or {}.",
            pipeline1, pipeline2);
        targetPipeline = pipeline1;
      } else {
        targetPipeline =
            !metric1.isGreater(metric2.get()) ? pipeline1 : pipeline2;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Compare the max datanode storage in the two pipelines, " +
                  "first : {}, second : {}, and chosen the {} pipeline = {}",
              metric1.get(), metric2.get(),
              targetPipeline == pipeline1 ? "first" : "second", targetPipeline);
        }
      }
    }
    return targetPipeline;
  }

  private SCMNodeMetric getMaxUsageNodeFromPipeline(Pipeline pipeline) {
    return pipeline.getNodes().stream()
        .map(nodeManager::getNodeStat)
        .filter(Objects::nonNull)
        .reduce((metric1, metric2) ->
            metric2.isGreater(metric1.get()) ? metric2 : metric1)
        .orElse(null);
  }

  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    List<Pipeline> mutableList = new ArrayList<>(pipelineList);
    Pipeline pipeline = choosePipeline(mutableList, pri);
    return pipelineList.indexOf(pipeline);
  }
}
