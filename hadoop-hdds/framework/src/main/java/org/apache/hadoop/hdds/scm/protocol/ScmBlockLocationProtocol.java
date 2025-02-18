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

package org.apache.hadoop.hdds.scm.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.security.KerberosInfo;

/**
 * ScmBlockLocationProtocol is used by an HDFS node to find the set of nodes
 * to read/write a block.
 */
@KerberosInfo(serverPrincipal = ScmConfig.ConfigStrings
      .HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
public interface ScmBlockLocationProtocol extends Closeable {

  @SuppressWarnings("checkstyle:ConstantName")
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   * @param size - size of the block.
   * @param numBlocks - number of blocks.
   * @param type - replication type of the blocks.
   * @param factor - replication factor of the blocks.
   * @param excludeList List of datanodes/containers to exclude during block
   *                    allocation.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Deprecated
  default List<AllocatedBlock> allocateBlock(long size, int numBlocks,
      ReplicationType type, ReplicationFactor factor, String owner,
      ExcludeList excludeList) throws IOException, TimeoutException {
    return allocateBlock(size, numBlocks, ReplicationConfig
        .fromProtoTypeAndFactor(type, factor), owner, excludeList);
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   *
   * @param size              - size of the block.
   * @param numBlocks         - number of blocks.
   * @param replicationConfig - replicationConfiguration
   * @param owner             - service owner of the new block
   * @param excludeList       List of datanodes/containers to exclude during
   *                          block
   *                          allocation.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  default List<AllocatedBlock> allocateBlock(long size, int numBlocks,
       ReplicationConfig replicationConfig, String owner,
       ExcludeList excludeList) throws IOException {
    return allocateBlock(size, numBlocks, replicationConfig, owner,
        excludeList, null);
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block, sorted
   * based on the client address.
   *
   * @param size              - size of the block.
   * @param numBlocks         - number of blocks.
   * @param replicationConfig - replicationConfiguration
   * @param owner             - service owner of the new block
   * @param excludeList       List of datanodes/containers to exclude during
   *                          block
   *                          allocation.
   * @param clientMachine client address, depends, can be hostname or
   *                      ipaddress.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  List<AllocatedBlock> allocateBlock(long size, int numBlocks,
      ReplicationConfig replicationConfig, String owner,
      ExcludeList excludeList, String clientMachine) throws IOException;

  /**
   * Delete blocks for a set of object keys.
   *
   * @param keyBlocksInfoList Map of object key and its blocks.
   * @return list of block deletion results.
   * @throws IOException if there is any failure.
   */
  List<DeleteBlockGroupResult>
      deleteKeyBlocks(List<BlockGroup> keyBlocksInfoList) throws IOException;

  /**
   * Gets the Clusterid and SCM Id from SCM.
   */
  ScmInfo getScmInfo() throws IOException;

  /**
   * Request to add SCM instance to HA group.
   */
  boolean addSCM(AddSCMRequest request) throws IOException;

  /**
   * Sort datanodes with distance to client.
   * @param nodes list of network name of each node.
   * @param clientMachine client address, depends, can be hostname or ipaddress.
   */
  List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) throws IOException;

  /**
   * Retrieves the hierarchical cluster tree representing the network topology.
   * @return the root node of the network topology cluster tree.
   * @throws IOException
   */
  InnerNode getNetworkTopology() throws IOException;
}
