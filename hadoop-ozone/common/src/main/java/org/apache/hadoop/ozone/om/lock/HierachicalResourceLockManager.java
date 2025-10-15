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

/**
 * Interface for Hierachical Resource Lock where the lock order acquired on resource is going to be deterministic and
 * there is no cyclic lock ordering on resources.
 * Typically, this can be used for locking elements which form a DAG like structure.(E.g. FSO tree, Snapshot chain etc.)
 */
public interface HierachicalResourceLockManager {

  HierarchicalResourceLock acquireLock(Resource resource, String key);

  /**
   * Interface for managing the lock lifecycle corresponding to a Hierarchical Resource.
   */
  interface HierarchicalResourceLock extends AutoCloseable {
    boolean isLockAcquired();

    @Override
    void close();
  }
}
