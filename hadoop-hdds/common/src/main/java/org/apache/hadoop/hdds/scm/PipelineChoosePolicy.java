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

package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.util.List;
import java.util.Map;

/**
 * A {@link PipelineChoosePolicy} support choosing pipeline from exist list.
 */
public interface PipelineChoosePolicy {

  String PIPELINE_CHOOSE_POLICY_PARAM_SIZE =
      "pipeline_choose_policy_param_size";

  /**
   * Given an initial list of pipelines, return one of the pipelines.
   *
   * @param pipelineList list of pipelines.
   * @return one of the pipelines.
   */
  Pipeline choosePipeline(List<Pipeline> pipelineList,
      Map<String, Object> params);
}
