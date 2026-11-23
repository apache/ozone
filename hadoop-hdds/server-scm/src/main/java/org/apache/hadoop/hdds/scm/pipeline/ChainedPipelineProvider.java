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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ChainedReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Implements Api for creating CHAINED (V3) pipelines.
 * Only factor THREE is supported.
 * All the datanodes in a CHAINED pipeline must have the
 * {@link DatanodeDetails.Port.Name#RATIS_DATASTREAM} port.
 */
public class ChainedPipelineProvider extends NCopyPipelineProvider<ChainedReplicationConfig> {

  private static final Set<ReplicationFactor> supportedReplicationFactors =
      Sets.newHashSet(ReplicationFactor.THREE, ReplicationFactor.ONE);

  private final ScmVersionManager versionManager;

  public ChainedPipelineProvider(NodeManager nodeManager,
                                 PipelineStateManager stateManager,
                                 ConfigurationSource conf,
                                 EventPublisher eventPublisher,
                                 SCMContext scmContext,
                                 ScmVersionManager versionManager) {
    super(nodeManager, stateManager, conf, eventPublisher, scmContext);
    this.versionManager = versionManager;
  }

  private void checkChainedPipelinesEnabled() throws SCMException {
    if (versionManager == null || !versionManager.isAllowed(HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT)) {
      throw new SCMException("Cluster is not finalized yet, it is"
          + " not enabled to create pipelines with CHAINED replication type.",
          SCMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  @Override
  public synchronized Pipeline create(ChainedReplicationConfig replicationConfig,
      List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes)
      throws IOException {
    checkChainedPipelinesEnabled();
    final ReplicationFactor factor = replicationConfig.getReplicationFactor();
    if (!supportedReplicationFactors.contains(factor)) {
      throw new IllegalArgumentException("Unsupported factor " + factor + " for " + replicationConfig);
    }
    final List<DatanodeDetails> dns = chooseThreeFactorDatanodes(excludedNodes, favoredNodes, factor.getNumber(),
        true, false);
    return createPipeline(replicationConfig, dns);
  }

  @Override
  public Pipeline create(ChainedReplicationConfig replicationConfig, List<DatanodeDetails> nodes) {
    try {
      checkChainedPipelinesEnabled();
    } catch (SCMException e) {
      throw new IllegalStateException(e);
    }
    return super.create(replicationConfig, nodes);
  }
}
