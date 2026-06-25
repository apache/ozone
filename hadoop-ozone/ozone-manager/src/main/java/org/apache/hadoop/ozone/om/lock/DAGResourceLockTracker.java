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

package org.apache.hadoop.ozone.om.lock;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

/**
 * Specialized implementation of {@link ResourceLockTracker} to manage locks for
 * {@link DAGLeveledResource} types. This class enforces hierarchical locking
 * constraints based on a directed acyclic graph (DAG) structure, ensuring that
 * resources are locked and unlocked in a consistent manner.
 *
 * This tracker maintains an adjacency set representing resource dependencies, wherein
 * each resource's children in the DAG define locking constraints. The class
 * populates these constraints at initialization and checks them dynamically when locks
 * are requested.
 *
 * The class is implemented as a singleton to ensure a central mechanism for
 * managing resource locks across threads. It uses ThreadLocal storage to track the
 * number of locks held for each resource by the current thread.
 *
 * Key Features:
 * 1. Hierarchical Locking: Resources are locked in the order defined by their DAG
 *    structure. Violating dependencies will result in denial of the lock request.
 * 2. Thread-safe: The class guarantees thread safety with synchronized initialization
 *    and thread-local management of lock counts.
 * 3. Deadlock Avoidance: Lock dependency constraints prevent circular dependencies
 *    in resource locking.
 * 4. Centralized Tracking: Tracks the current locks held across all DAGLeveledResource
 *    instances and provides utilities to inspect current lock states.
 *
 * Thread safety: This class is thread-safe.
 */
final class DAGResourceLockTracker extends ResourceLockTracker<DAGLeveledResource> {

  private final EnumMap<DAGLeveledResource, ThreadLocal<Integer>> acquiredLocksMap =
      new EnumMap<>(DAGLeveledResource.class);
  private final Map<DAGLeveledResource, Set<DAGLeveledResource>> lockDependentAdjacencySet =
      new EnumMap<>(DAGLeveledResource.class);

  private static volatile DAGResourceLockTracker instance = null;

  public static DAGResourceLockTracker get() {
    if (instance == null) {
      synchronized (DAGResourceLockTracker.class) {
        if (instance == null) {
          instance = new DAGResourceLockTracker();
        }
      }
    }
    return instance;
  }

  /**
   * Performs a Depth-First Search (DFS) traversal on a directed acyclic graph (DAG)
   * composed of {@code DAGLeveledResource} objects. This method populates a mapping
   * of resource dependencies while traversing the graph.
   *
   * The traversal stops for any resource already visited, avoiding potential cycles
   * (though cycles should not exist in a valid DAG). Throws an exception if the provided
   * stack is not empty at the start of the traversal.
   *
   * @param resource The starting resource for the DFS traversal.
   * @param stack A stack used to manage the traversal process. The stack should be empty
   *              when passed in.
   * @param visited A set of resources that have already been visited during the traversal
   *                to ensure each resource is processed only once.
   */
  private void dfs(DAGLeveledResource resource, Stack<DAGLeveledResource> stack, Set<DAGLeveledResource> visited) {
    if (visited.contains(resource)) {
      return;
    }
    if (!stack.isEmpty()) {
      throw new IllegalStateException("Stack is not empty while beginning to traverse the DAG for resource :"
          + resource);
    }
    stack.push(resource);
    while (!stack.isEmpty()) {
      DAGLeveledResource current = stack.peek();
      if (!visited.contains(current)) {
        visited.add(current);
        for (DAGLeveledResource child : current.getChildren()) {
          stack.push(child);
        }
      } else {
        if (!lockDependentAdjacencySet.containsKey(current)) {
          Set<DAGLeveledResource> adjacentResources = null;
          for (DAGLeveledResource child : current.getChildren()) {
            if (adjacentResources == null) {
              adjacentResources = new HashSet<>();
            }
            adjacentResources.add(child);
            adjacentResources.addAll(lockDependentAdjacencySet.get(child));
          }
          lockDependentAdjacencySet.put(current,
              adjacentResources == null ? Collections.emptySet() : adjacentResources);
        }
        stack.pop();
      }
    }
  }

  private void populateLockDependentAdjacencySet() {
    Set<DAGLeveledResource> visited = new HashSet<>();
    Stack<DAGLeveledResource> stack = new Stack<>();
    for (DAGLeveledResource resource : DAGLeveledResource.values()) {
      dfs(resource, stack, visited);
    }
  }

  private DAGResourceLockTracker() {
    populateLockDependentAdjacencySet();
    for (DAGLeveledResource dagLeveledResource : DAGLeveledResource.values()) {
      acquiredLocksMap.put(dagLeveledResource, ThreadLocal.withInitial(() -> 0));
    }
  }

  private void updateAcquiredLockCount(DAGLeveledResource resource, int delta) {
    acquiredLocksMap.get(resource).set(acquiredLocksMap.get(resource).get() + delta);
  }

  @Override
  OMLockDetails lockResource(DAGLeveledResource resource) {
    updateAcquiredLockCount(resource, 1);
    return super.lockResource(resource);
  }

  @Override
  OMLockDetails unlockResource(DAGLeveledResource resource) {
    updateAcquiredLockCount(resource, -1);
    return super.unlockResource(resource);
  }

  @Override
  public boolean canLockResource(DAGLeveledResource resource) {
    for (DAGLeveledResource child : lockDependentAdjacencySet.get(resource)) {
      if (acquiredLocksMap.get(child).get() > 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  Stream<DAGLeveledResource> getCurrentLockedResources() {
    return acquiredLocksMap.keySet().stream().filter(dagLeveled -> acquiredLocksMap.get(dagLeveled).get() > 0);
  }
}
