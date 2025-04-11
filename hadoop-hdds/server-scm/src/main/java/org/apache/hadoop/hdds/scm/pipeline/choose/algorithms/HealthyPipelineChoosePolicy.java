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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * The healthy pipeline choose policy that chooses pipeline
 * until return healthy pipeline.
 */
public class HealthyPipelineChoosePolicy implements PipelineChoosePolicy {

  private PipelineChoosePolicy randomPolicy = new RandomPipelineChoosePolicy();

  @Override
  public Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    Pipeline fallback = null;
    while (!pipelineList.isEmpty()) {
      Pipeline pipeline = randomPolicy.choosePipeline(pipelineList, pri);
      if (pipeline.isHealthy()) {
        return pipeline;
      } else {
        fallback = pipeline;
        pipelineList.remove(pipeline);
      }
    }
    return fallback;
  }

  @Override
  public int choosePipelineIndex(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    List<Pipeline> mutableList = new ArrayList<>(pipelineList);
    Pipeline pipeline = choosePipeline(mutableList, pri);
    return pipelineList.indexOf(pipeline);
  }
}
