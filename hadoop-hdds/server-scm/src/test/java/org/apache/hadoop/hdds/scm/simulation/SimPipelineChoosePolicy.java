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

package org.apache.hadoop.hdds.scm.simulation;

import java.util.List;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Picks a random pipeline like SCM's default policy, with the random choices of the running simulation, so that EC
 * block allocation picks pipelines reproducibly. SCM loads it through {@code hdds.scm.ec.pipeline.choose.policy.impl}.
 */
public class SimPipelineChoosePolicy implements PipelineChoosePolicy {

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList, PipelineRequestInformation pri) {
    int index = choosePipelineIndex(pipelineList, pri);
    return index < 0 ? null : pipelineList.get(index);
  }

  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList, PipelineRequestInformation pri) {
    if (pipelineList == null || pipelineList.isEmpty()) {
      return -1;
    }
    return ScmSimulation.running().random(SimRandom.SCM).nextInt(pipelineList.size());
  }
}
