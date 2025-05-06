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

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline choose policy that randomly choose pipeline with relatively
 * lower utilization.
 * <p>
 * The Algorithm is as follows, Pick 2 random pipelines from a given pool of
 * pipelines and then pick the pipeline which has lower utilization.
 * This leads to a higher probability of pipelines with lower utilization
 * to be picked.
 * <p>
 * For those wondering why we choose two pipelines randomly and choose the
 * pipeline with lower utilization. There are links to this original papers in
 * HDFS-11564.
 * Also, the same algorithm applies to SCMContainerPlacementCapacity.
 * <p>
 */
public class CapacityPipelineChoosePolicy implements PipelineChoosePolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineChoosePolicy.class);

  private NodeManager nodeManager;

  private final PipelineChoosePolicy healthPolicy;

  public CapacityPipelineChoosePolicy() {
    healthPolicy = new HealthyPipelineChoosePolicy();
  }

  @Override
  public PipelineChoosePolicy init(final NodeManager scmNodeManager) {
    this.nodeManager = scmNodeManager;
    return this;
  }

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    Pipeline pipeline1 = healthPolicy.choosePipeline(pipelineList, pri);
    Pipeline pipeline2 = healthPolicy.choosePipeline(pipelineList, pri);

    int result = new CapacityPipelineComparator(this)
        .compare(pipeline1, pipeline2);

    LOG.debug("Chosen the {} pipeline", result <= 0 ? "first" : "second");
    return result <= 0 ? pipeline1 : pipeline2;
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

      // Compare the scmUsed weight of the node in the two sorted node stacks
      LOG.debug("Compare scmUsed weight in pipelines, first : {}, second : {}",
          sortedNodes1, sortedNodes2);
      int result = 0;
      int count = 0;
      while (result == 0 &&
          !sortedNodes1.isEmpty() && !sortedNodes2.isEmpty()) {
        count++;
        LOG.debug("Compare {} round", count);
        result = sortedNodes1.pop().compareTo(sortedNodes2.pop());
      }
      return result;
    }
  }

}
