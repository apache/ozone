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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * A PlacementPolicy support choosing datanodes to build
 * pipelines or containers with specified constraints.
 */
public interface PlacementPolicy {

  default List<DatanodeDetails> chooseDatanodes(
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes, int nodesRequired,
          long metadataSizeRequired, long dataSizeRequired) throws IOException {
    return this.chooseDatanodes(Collections.emptyList(), excludedNodes,
            favoredNodes, nodesRequired, metadataSizeRequired,
            dataSizeRequired);
  }

  /**
   * Given an initial set of datanodes and the size required,
   * return set of datanodes that satisfy the nodes and size requirement.
   *
   * @param usedNodes - List of nodes already chosen for pipeline
   * @param excludedNodes - list of nodes to be excluded.
   * @param favoredNodes - list of nodes preferred.
   * @param nodesRequired - number of datanodes required.
   * @param dataSizeRequired - size required for the container.
   * @param metadataSizeRequired - size required for Ratis metadata.
   * @return list of datanodes chosen.
   * @throws IOException
   */
  List<DatanodeDetails> chooseDatanodes(List<DatanodeDetails> usedNodes,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes,
          int nodesRequired, long metadataSizeRequired,
          long dataSizeRequired) throws IOException;

  /**
   * Given a list of datanode and the number of replicas required, return
   * a PlacementPolicyStatus object indicating if the container meets the
   * placement policy - ie is it on the correct number of racks, etc.
   * @param dns List of replica holding a replica of the container
   * @param replicas The expected number of replicas
   */
  ContainerPlacementStatus validateContainerPlacement(
          List<DatanodeDetails> dns, int replicas);

  /**
   * Given a set of replicas of a container which are
   * neither over underreplicated nor overreplicated,
   * return a set of replicas to copy to another node to fix misreplication.
   * @param replicas Map of replicas with value signifying if
   *                  replica can be copied
   */
  Set<ContainerReplica> replicasToCopyToFixMisreplication(
          Map<ContainerReplica, Boolean> replicas);

  /**
   * Given a set of replicas of a container which are overreplicated,
   * return a set of replicas to delete to fix overreplication.
   * @param replicas Set of existing replicas of the container
   * @param expectedCountPerUniqueReplica Replication factor of each
   *                                     unique replica
   */
  Set<ContainerReplica> replicasToRemoveToFixOverreplication(
          Set<ContainerReplica> replicas, int expectedCountPerUniqueReplica);
}
