/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.ratis.RatisHelper;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for Ratis pipelines. Contains methods to create and destroy
 * ratis pipelines.
 */
public final class RatisPipelineUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(RatisPipelineUtils.class);

  private RatisPipelineUtils() {
  }
  /**
   * Removes pipeline from SCM. Sends ratis command to destroy pipeline on all
   * the datanodes.
   *
   * @param pipeline        - Pipeline to be destroyed
   * @param ozoneConf       - Ozone configuration
   * @throws IOException
   */
  static void destroyPipeline(Pipeline pipeline, Configuration ozoneConf) {
    final RaftGroup group = RatisHelper.newRaftGroup(pipeline);
    LOG.debug("destroying pipeline:{} with {}", pipeline.getId(), group);
    for (DatanodeDetails dn : pipeline.getNodes()) {
      try {
        destroyPipeline(dn, pipeline.getId(), ozoneConf);
      } catch (IOException e) {
        LOG.warn("Pipeline destroy failed for pipeline={} dn={}",
            pipeline.getId(), dn);
      }
    }
  }

  /**
   * Sends ratis command to destroy pipeline on the given datanode.
   *
   * @param dn         - Datanode on which pipeline needs to be destroyed
   * @param pipelineID - ID of pipeline to be destroyed
   * @param ozoneConf  - Ozone configuration
   * @throws IOException
   */
  static void destroyPipeline(DatanodeDetails dn, PipelineID pipelineID,
      Configuration ozoneConf) throws IOException {
    final String rpcType = ozoneConf
        .get(ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
            ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT);
    final RetryPolicy retryPolicy = RatisHelper.createRetryPolicy(ozoneConf);
    final RaftPeer p = RatisHelper.toRaftPeer(dn);
    final int maxOutstandingRequests =
        HddsClientUtils.getMaxOutstandingRequests(ozoneConf);
    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(
        new SecurityConfig(ozoneConf));
    final TimeDuration requestTimeout =
        RatisHelper.getClientRequestTimeout(ozoneConf);
    RaftClient client = RatisHelper
        .newRaftClient(SupportedRpcType.valueOfIgnoreCase(rpcType), p,
            retryPolicy, maxOutstandingRequests, tlsConfig, requestTimeout);
    client
        .groupRemove(RaftGroupId.valueOf(pipelineID.getId()), true, p.getId());
  }
}
