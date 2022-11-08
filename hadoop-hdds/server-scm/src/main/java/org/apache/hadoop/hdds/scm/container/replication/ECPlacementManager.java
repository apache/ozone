/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.hadoop.hdds.JavaUtils.getReverseMapSet;

/**
 * Class to handle placement of Replicas.
 * @param <G> Placement Group
 */
public class ECPlacementManager<G> {

  private PlacementPolicy<G> placementPolicy;
  public ECPlacementManager(PlacementPolicy<G> placementPolicy) {
    this.placementPolicy = placementPolicy;
  }

  private void removeReplicaIndex(int replicaIndex,
          Map<G, Set<Integer>> placementGroupReplicaIndexMap,
          Map<Integer, Set<G>> replicaIndexPlacementGroupMap,
          ConcurrentSkipListSet<G> placementGroupSet) {
    for (G placementGroup : replicaIndexPlacementGroupMap.get(replicaIndex)) {
      placementGroupSet.remove(placementGroup);
      placementGroupReplicaIndexMap.get(placementGroup).remove(replicaIndex);
      placementGroupSet.add(placementGroup);
    }
  }

  /**
   * HDDS-6966: Getting the placement group for each of the replica
   * (Rack for SCMCommonPlacementPolicy).
   *
   * @param replicaSources
   * @return Placement group mapping to the set of replica indexes it holds
   */
  public Map<G, Set<Integer>> getPlacementGroupReplicaIndexMap(
          List<ContainerReplica> replicaSources) {
    Map<G, Set<Integer>> placementGroupReplicaIndexMap = new HashMap<>();
    for (ContainerReplica replica : replicaSources) {
      G placementGroup = placementPolicy.getPlacementGroup(
              replica.getDatanodeDetails());
      if (!placementGroupReplicaIndexMap.containsKey(placementGroup)) {
        placementGroupReplicaIndexMap.put(placementGroup, new HashSet<>());
      }
      placementGroupReplicaIndexMap.get(placementGroup)
              .add(replica.getReplicaIndex());
    }
    return placementGroupReplicaIndexMap;
  }

  /**
   * Get Misreplicated Replica Indexes in sorted order based on the replica
   * index's spread across different placement group.
   * @param placementGroupReplicaIndexMap
   * @param excludedIndexes
   * @return List of Misreplicated Indexes
   */
  public List<Integer> getMisreplicatedIndexes(
          Map<G, Set<Integer>> placementGroupReplicaIndexMap,
          Set<Integer> excludedIndexes) {
    for (Set<Integer> placementGroupIndexes :
            placementGroupReplicaIndexMap.values()) {
      placementGroupIndexes.removeAll(excludedIndexes);
    }
    Map<Integer, Set<G>> replicaIndexPlacementGroupMap =
            getReverseMapSet(placementGroupReplicaIndexMap);

    ConcurrentSkipListSet<G> placementGroupSet = new ConcurrentSkipListSet<>(
            Comparator.comparingInt(g -> placementGroupReplicaIndexMap.get(g)
                    .size()).thenComparing(Object::toString));
    placementGroupSet.addAll(placementGroupReplicaIndexMap.keySet());
    ConcurrentSkipListSet<Integer> replicaSet = new ConcurrentSkipListSet<>(
            Comparator.comparingInt(idx ->
                            replicaIndexPlacementGroupMap
                            .getOrDefault(idx, Collections.emptySet())
                            .size()).thenComparingInt(idx -> (int) idx));
    replicaSet.addAll(replicaIndexPlacementGroupMap.keySet());
    List<Integer> misreplicatedIndexes = new ArrayList<>();

    while (replicaSet.size() > 0) {
      Optional<Integer> replicaIndex;
      if (placementGroupReplicaIndexMap.get(placementGroupSet.first())
              .size() <= 1) {
        G placementGroup = placementGroupSet.pollFirst();
        replicaIndex = placementGroupReplicaIndexMap
                .get(placementGroup).stream().findFirst();
        replicaIndex.ifPresent(replicaSet::remove);
      } else {
        replicaIndex = Optional.ofNullable(replicaSet.pollFirst());
        misreplicatedIndexes.add(replicaIndex.get());
      }

      if (replicaIndex.isPresent()) {
        int replicaIdx = replicaIndex.get();
        this.removeReplicaIndex(replicaIdx, placementGroupReplicaIndexMap,
                replicaIndexPlacementGroupMap, placementGroupSet);
      }
    }
    return misreplicatedIndexes;
  }

  public List<Integer> getMisreplicatedIndexes(
          List<ContainerReplica> replicaSources,
          Set<Integer> excludedIndexes) {
    return this.getMisreplicatedIndexes(
            this.getPlacementGroupReplicaIndexMap(replicaSources),
            excludedIndexes);
  }
}
