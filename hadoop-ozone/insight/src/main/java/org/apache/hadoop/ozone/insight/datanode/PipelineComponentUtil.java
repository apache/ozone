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

package org.apache.hadoop.ozone.insight.datanode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.insight.Component;
import org.apache.hadoop.ozone.insight.Component.Type;

/**
 * Utilities to handle pipelines.
 */
public final class PipelineComponentUtil {

  public static final String PIPELINE_FILTER = "pipeline";

  private PipelineComponentUtil() {
  }

  public static String getPipelineIdFromFilters(Map<String, String> filters) {
    if (filters == null || !filters.containsKey(PIPELINE_FILTER)) {
      throw new IllegalArgumentException(PIPELINE_FILTER
          + " filter should be specified (-f " + PIPELINE_FILTER
          + "=<pipelineid)");
    }

    return filters.get(PIPELINE_FILTER);
  }

  /**
   * Execute a function with each of the datanodes.
   */
  public static void withDatanodesFromPipeline(
      ScmClient scmClient,
      String pipelineId,
      Function<Component, Void> func
  ) throws IOException {

    Optional<Pipeline> pipelineSelection = scmClient.listPipelines()
        .stream()
        .filter(
            pipline -> pipline.getId().getId().toString().equals(pipelineId))
        .findFirst();

    if (!pipelineSelection.isPresent()) {
      throw new IllegalArgumentException("No such multi-node pipeline.");
    }
    Pipeline pipeline = pipelineSelection.get();
    for (DatanodeDetails datanode : pipeline.getNodes()) {
      Component dn =
          new Component(Type.DATANODE, datanode.getUuid().toString(),
              datanode.getHostName(), 9882);
      func.apply(dn);
    }

  }
}
