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

import java.io.IOException;
import java.util.stream.Stream;

/**
 * A read only lock manager that does not acquire any lock.
 */
public class ReadOnlyHierarchicalResourceLockManager implements HierarchicalResourceLockManager {

  private static final HierarchicalResourceLock EMPTY_LOCK_ACQUIRED = new HierarchicalResourceLock() {
    @Override
    public boolean isLockAcquired() {
      return true;
    }

    @Override
    public void close() {

    }
  };

  private static final HierarchicalResourceLock EMPTY_LOCK_NOT_ACQUIRED = new HierarchicalResourceLock() {
    @Override
    public boolean isLockAcquired() {
      return false;
    }

    @Override
    public void close() {
    }
  };

  @Override
  public HierarchicalResourceLock acquireReadLock(DAGLeveledResource resource, String key) throws IOException {
    return EMPTY_LOCK_ACQUIRED;
  }

  @Override
  public HierarchicalResourceLock acquireWriteLock(DAGLeveledResource resource, String key) throws IOException {
    return EMPTY_LOCK_NOT_ACQUIRED;
  }

  @Override
  public HierarchicalResourceLock acquireResourceWriteLock(DAGLeveledResource resource) throws IOException {
    return EMPTY_LOCK_NOT_ACQUIRED;
  }

  @Override
  public Stream<DAGLeveledResource> getCurrentLockedResources() {
    return Stream.empty();
  }

  @Override
  public void close() throws Exception {

  }
}
