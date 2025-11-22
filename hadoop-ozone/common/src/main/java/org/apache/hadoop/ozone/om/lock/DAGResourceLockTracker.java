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
