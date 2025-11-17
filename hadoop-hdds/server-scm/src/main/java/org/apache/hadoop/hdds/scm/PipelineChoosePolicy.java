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

import java.util.List;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * A {@link PipelineChoosePolicy} support choosing pipeline from exist list.
 */
public interface PipelineChoosePolicy {

  /**
   * Updates the policy with NodeManager.
   * @return updated policy.
   */
  default PipelineChoosePolicy init(final NodeManager nodeManager) {
    // override if the policy requires nodeManager
    return this;
  }

  /**
   * Given an initial list of pipelines, return one of the pipelines.
   *
   * @param pipelineList list of pipelines.
   * @return one of the pipelines or null if no pipeline can be selected.
   */
  Pipeline choosePipeline(List<Pipeline> pipelineList,
      PipelineRequestInformation pri);

  /**
   * Given a list of pipelines, return the index of the chosen pipeline.
   * @param pipelineList List of pipelines
   * @param pri          PipelineRequestInformation
   * @return Index in the list of the chosen pipeline, or -1 if no pipeline
   *         could be selected.
   */
  default int choosePipelineIndex(List<Pipeline> pipelineList,
      PipelineRequestInformation pri) {
    return pipelineList == null || pipelineList.isEmpty() ? -1 : 0;
  }
}
