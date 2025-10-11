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

import java.util.List;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Round-robin choose policy that chooses pipeline in a round-robin fashion.
 * Only useful for debugging and testing purposes, at least for now.
 */
public class RoundRobinPipelineChoosePolicy implements PipelineChoosePolicy {

  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinPipelineChoosePolicy.class);

  // Stores the index of the next pipeline to be returned.
  private int nextPipelineIndex = 0;

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    return pipelineList.get(choosePipelineIndex(pipelineList, pri));
  }

  /**
   * Given a list of pipelines, return the index of the chosen pipeline.
   * @param pipelineList List of pipelines
   * @param pri          PipelineRequestInformation
   * @return Index in the list of the chosen pipeline.
   */
  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    final int numPipelines = pipelineList.size();
    int chosenIndex;
    synchronized (this) {
      nextPipelineIndex = nextPipelineIndex % numPipelines;
      chosenIndex = nextPipelineIndex++;
    }
    LOG.debug("chosenIndex = {}, numPipelines = {}", chosenIndex, numPipelines);
    return chosenIndex;
  }
}
