/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * Interface for creating pipelines.
 */
public abstract class PipelineProvider {

  private final NodeManager nodeManager;
  private final PipelineStateManager stateManager;

  public PipelineProvider(NodeManager nodeManager,
      PipelineStateManager stateManager) {
    this.nodeManager = nodeManager;
    this.stateManager = stateManager;
  }

  public PipelineProvider() {
    this.nodeManager = null;
    this.stateManager = null;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public PipelineStateManager getPipelineStateManager() {
    return stateManager;
  }

  protected abstract Pipeline create(int replication)
      throws IOException;

  protected abstract Pipeline create(int replication,
      List<DatanodeDetails> nodes);

  protected abstract void close(Pipeline pipeline) throws IOException;

  protected abstract void shutdown();

  List<DatanodeDetails> pickNodesNeverUsed(ReplicationType type,
      int replication) throws SCMException {
    Set<DatanodeDetails> dnsUsed = new HashSet<>();
    stateManager.getPipelines(type, replication).stream().filter(
        p -> p.getPipelineState().equals(Pipeline.PipelineState.OPEN) ||
            p.getPipelineState().equals(Pipeline.PipelineState.DORMANT) ||
            p.getPipelineState().equals(Pipeline.PipelineState.ALLOCATED))
        .forEach(p -> dnsUsed.addAll(p.getNodes()));

    // Get list of healthy nodes
    List<DatanodeDetails> dns = nodeManager
        .getNodes(HddsProtos.NodeState.HEALTHY)
        .parallelStream()
        .filter(dn -> !dnsUsed.contains(dn))
        .limit(replication)
        .collect(Collectors.toList());
    if (dns.size() < replication) {
      String e = String
          .format("Cannot create pipeline of factor %d using %d nodes." +
                  " Used %d nodes. Healthy nodes %d", replication,
              dns.size(), dnsUsed.size(),
              nodeManager.getNodes(HddsProtos.NodeState.HEALTHY).size());
      throw new SCMException(e,
          SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE);
    }
    return dns;
  }
}
