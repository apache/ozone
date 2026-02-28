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

import java.io.Closeable;
import java.io.IOException;
import java.util.stream.Stream;

/**
 * Interface for Hierachical Resource Lock where the lock order acquired on resource is going to be deterministic and
 * there is no cyclic lock ordering on resources.
 * Typically, this can be used for locking elements which form a DAG like structure.(E.g. FSO tree, Snapshot chain etc.)
 */
public interface HierarchicalResourceLockManager extends AutoCloseable {

  /**
   * Acquires a read lock on the specified resource using the provided key.
   *
   * @param resource the resource on which the read lock is to be acquired
   * @param key a unique identifier used for managing the lock
   * @return a {@code HierarchicalResourceLock} interface to manage the lifecycle of the acquired lock
   * @throws IOException if an I/O error occurs during the process of acquiring the lock
   */
  HierarchicalResourceLock acquireReadLock(DAGLeveledResource resource, String key) throws IOException;

  /**
   * Acquires a write lock on the specified resource using the provided key.
   *
   * @param resource the resource on which the write lock is to be acquired
   * @param key a unique identifier used for managing the lock
   * @return a {@code HierarchicalResourceLock} interface to manage the lifecycle of the acquired lock
   * @throws IOException if an I/O error occurs during the process of acquiring the lock
   */
  HierarchicalResourceLock acquireWriteLock(DAGLeveledResource resource, String key) throws IOException;

  /**
   * Acquires a write lock on the specified hierarchical resource.
   * This method is typically used for managing write operations on resources
   * that are part of a directed acyclic graph (DAG) structure, ensuring a
   * deterministic and deadlock-free locking order.
   *
   * @param resource the {@code DAGLeveledResource} representing the hierarchical
   *                 resource for which the write lock is to be acquired
   * @return a {@code HierarchicalResourceLock} instance to manage the lifecycle
   *         of the acquired lock for the specified resource
   * @throws IOException if an I/O error occurs during the lock acquisition process
   */
  HierarchicalResourceLock acquireResourceWriteLock(DAGLeveledResource resource) throws IOException;

  /**
   * Retrieves a stream of all currently locked resources in the system by the thread calling this method.
   *
   * @return a stream of {@code DAGLeveledResource} representing the resources that are currently locked
   */
  Stream<DAGLeveledResource> getCurrentLockedResources();

  /**
   * Interface for managing the lock lifecycle corresponding to a Hierarchical Resource.
   */
  interface HierarchicalResourceLock extends Closeable {
    boolean isLockAcquired();
  }
}
