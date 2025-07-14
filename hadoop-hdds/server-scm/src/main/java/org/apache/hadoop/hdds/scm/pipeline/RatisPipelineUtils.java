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

package org.apache.hadoop.hdds.scm.pipeline;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;

/**
 * Utility class for Ratis pipelines.
 */
public final class RatisPipelineUtils {

  private RatisPipelineUtils() {
  }

  /**
   * Return the list of pipelines who share the same set of datanodes
   * with the input pipeline.
   *
   * @param stateManager PipelineStateManagerImpl
   * @param pipeline input pipeline
   * @return list of matched pipeline
   */
  static List<Pipeline> checkPipelineContainSameDatanodes(
      PipelineStateManager stateManager, Pipeline pipeline) {
    return stateManager
        .getPipelines(RatisReplicationConfig
            .getInstance(ReplicationFactor.THREE))
        .stream().filter(p -> !p.getId().equals(pipeline.getId()) &&
            (p.getPipelineState() != Pipeline.PipelineState.CLOSED &&
                p.sameDatanodes(pipeline)))
        .collect(Collectors.toList());
  }
}
