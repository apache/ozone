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

package org.apache.hadoop.hdds.scm.node.states;

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * This data structure maintains the list of pipelines which the given
 * datanode is a part of. This information will be added whenever a new
 * pipeline allocation happens.
 *
 * <p>TODO: this information needs to be regenerated from pipeline reports
 * on SCM restart
 */
public class Node2PipelineMap {
  private final Map<DatanodeID, Set<PipelineID>> dn2PipelineMap = new ConcurrentHashMap<>();

  /**
   * Constructs a Node2PipelineMap Object.
   */
  public Node2PipelineMap() {
  }

  /**
   * Returns null if there are no pipelines associated with this datanode ID.
   *
   * @param datanode - DatanodeID
   * @return Set of pipelines or Null.
   */
  public Set<PipelineID> getPipelines(@Nonnull DatanodeID datanode) {
    final Set<PipelineID> s = dn2PipelineMap.get(datanode);
    return s != null ? new HashSet<>(s) : Collections.emptySet();
  }

  /**
   * Return 0 if there are no pipelines associated with this datanode ID.
   *
   * @param datanode - DatanodeID
   * @return Number of pipelines or 0.
   */
  public int getPipelinesCount(DatanodeID datanode) {
    return getPipelines(datanode).size();
  }

  /**
   * Adds a pipeline entry to a given dataNode in the map.
   *
   * @param pipeline Pipeline to be added
   */
  public void addPipeline(Pipeline pipeline) {
    for (DatanodeDetails details : pipeline.getNodes()) {
      DatanodeID dnId = details.getID();
      dn2PipelineMap.computeIfAbsent(dnId, k -> ConcurrentHashMap.newKeySet())
          .add(pipeline.getId());
    }
  }

  public void removePipeline(Pipeline pipeline) {
    for (DatanodeDetails details : pipeline.getNodes()) {
      DatanodeID dnId = details.getID();
      dn2PipelineMap.computeIfPresent(dnId,
          (k, v) -> {
            v.remove(pipeline.getId());
            return v;
          });
    }
  }
}
