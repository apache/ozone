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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
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

  private final PipelineChoosePolicy healthPolicy;

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

    int result = new CapacityPipelineComparator(this)
        .compare(pipeline1, pipeline2);
    targetPipeline = result <= 0 ? pipeline1 : pipeline2;

    LOG.debug("Chosen the {} pipeline by compared scmUsed",
        targetPipeline == pipeline1 ? "first" : "second");

    return targetPipeline;
  }

  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    List<Pipeline> mutableList = new ArrayList<>(pipelineList);
    Pipeline pipeline = choosePipeline(mutableList, pri);
    return pipelineList.indexOf(pipeline);
  }

  /**
   * Return a list of SCMNodeMetrics corresponding to the DataNodes in the
   * pipeline, sorted in descending order based on scm used storage.
   * @param pipeline pipeline
   * @return sorted SCMNodeMetrics corresponding the pipeline
   */
  private Deque<SCMNodeMetric> getSortedNodeFromPipeline(Pipeline pipeline) {
    Deque<SCMNodeMetric> sortedNodeStack = new ArrayDeque<>();
    pipeline.getNodes().stream()
        .map(nodeManager::getNodeStat)
        .filter(Objects::nonNull)
        .sorted()
        .forEach(sortedNodeStack::push);
    return sortedNodeStack;
  }

  static class CapacityPipelineComparator implements Comparator<Pipeline> {
    private final CapacityPipelineChoosePolicy policy;

    CapacityPipelineComparator(CapacityPipelineChoosePolicy policy) {
      this.policy = policy;
    }
    @Override
    public int compare(Pipeline p1, Pipeline p2) {
      if (p1.getId().equals(p2.getId())) {
        LOG.debug("Compare the same pipeline {}", p1);
        return 0;
      }
      Deque<SCMNodeMetric> sortedNodes1 = policy.getSortedNodeFromPipeline(p1);
      Deque<SCMNodeMetric> sortedNodes2 = policy.getSortedNodeFromPipeline(p2);

      if (sortedNodes1.isEmpty() || sortedNodes2.isEmpty()) {
        LOG.warn("Cannot obtain SCMNodeMetric in pipeline {} or {}", p1, p2);
        return 0;
      }
      LOG.debug("Compare scmUsed in pipelines, first : {}, second : {}",
          sortedNodes1, sortedNodes2);
      // Compare the scmUsed of the first node in the two sorted node stacks
      int result = sortedNodes1.pop().compareTo(sortedNodes2.pop());

      if (result == 0 && !sortedNodes1.isEmpty() && !sortedNodes2.isEmpty()) {
        // Compare the scmUsed of the second node in the two sorted node stacks
        LOG.debug("Secondary compare because the first round is the same");
        result = sortedNodes1.pop().compareTo(sortedNodes2.pop());
      }
      return result;
    }
  }

}
