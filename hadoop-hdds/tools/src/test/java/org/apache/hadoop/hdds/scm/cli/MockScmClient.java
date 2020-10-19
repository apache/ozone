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
package org.apache.hadoop.hdds.scm.cli;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This is a mock SCMClient to be used for CLI tests. All methods are mocked
 * and the intention is that individual tests should sub-class this mock and
 * implement only the methods the test cares about.
 */

public class MockScmClient implements ScmClient {
  @Override
  public ContainerWithPipeline createContainer(String owner)
      throws IOException {
    return null;
  }

  @Override
  public ContainerInfo getContainer(long containerId) throws IOException {
    return null;
  }

  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException {
    return null;
  }

  @Override
  public void closeContainer(long containerId) throws IOException {
  }

  @Override
  public void deleteContainer(
      long containerId, Pipeline pipeline, boolean force) throws IOException {
  }

  @Override
  public void deleteContainer(
      long containerId, boolean force) throws IOException {
  }

  @Override
  public List<ContainerInfo> listContainer(
      long startContainerID, int count) throws IOException {
    return null;
  }

  @Override
  public ContainerProtos.ContainerDataProto readContainer(
      long containerID, Pipeline pipeline) throws IOException {
    return null;
  }

  @Override
  public ContainerProtos.ContainerDataProto readContainer(
      long containerID) throws IOException {
    return null;
  }

  @Override
  public long getContainerSize(
      long containerID) throws IOException {
    return 0;
  }

  @Override
  public ContainerWithPipeline createContainer(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor replicationFactor,
      String owner) throws IOException {
    return null;
  }

  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState nodeState,
      HddsProtos.QueryScope queryScope, String poolName) throws IOException {
    return null;
  }

  @Override
  public void decommissionNodes(List<String> hosts) throws IOException {
  }

  @Override
  public void recommissionNodes(List<String> hosts) throws IOException {
  }

  @Override
  public void startMaintenanceNodes(List<String> hosts, int endHours)
      throws IOException {
  }

  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException {
    return null;
  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    return null;
  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    return null;
  }

  @Override
  public void activatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
  }

  @Override
  public boolean inSafeMode() throws IOException {
    return false;
  }

  @Override
  public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException {
    return null;
  }

  @Override
  public boolean forceExitSafeMode() throws IOException {
    return false;
  }

  @Override
  public void startReplicationManager() throws IOException {
  }

  @Override
  public void stopReplicationManager() throws IOException {
  }

  @Override
  public boolean getReplicationManagerStatus() throws IOException {
    return false;
  }

  @Override
  public void close() throws IOException {
  }
}
