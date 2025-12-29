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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock.Resource;

/**
 * The {@code DAGLeveledResource} enum represents a set of resources that are associated with
 * specific locks in the Ozone Manager Lock mechanism. These resources allow fine-grained
 * locking at various levels, ensuring consistent access and management of system data.
 * Each resource can optionally define child resources, forming a directed acyclic graph (DAG)
 * structure for hierarchical locking.
 * The enum implements the {@code Resource} interface, providing a name for identification and
 * an associated {@link IOzoneManagerLock.ResourceManager} to manage its locking behavior.
 * DAGLeveledResource defines the order in which resources can be locked. Attempting to lock the resources out of order
 * will throw a RuntimeException.
 * For instance, acquiring SNAPSHOT_DB_CONTENT_LOCK followed by SNAPSHOT_DB_LOCK is allowed; acquiring
 * SNAPSHOT_LOCAL_DATA_LOCK followed by SNAPSHOT_DB_CONTENT_LOCK is not permitted.
 */
public enum DAGLeveledResource implements Resource {
  // Background services lock on a Snapshot.
  SNAPSHOT_GC_LOCK("SNAPSHOT_GC_LOCK"),
  // Lock acquired on a Snapshot's RocksDB Handle.
  SNAPSHOT_DB_LOCK("SNAPSHOT_DB_LOCK"),
  // Lock acquired on a Snapshot's Local Data.
  SNAPSHOT_LOCAL_DATA_LOCK("SNAPSHOT_LOCAL_DATA_LOCK"),
  // Lock acquired on a Snapshot's RocksDB contents. (This lock should be always acquired before opening a snapshot
  // which acquires a SNAPSHOT_DB_LOCK)
  SNAPSHOT_DB_CONTENT_LOCK("SNAPSHOT_DB_CONTENT_LOCK",
      new DAGLeveledResource[] {SNAPSHOT_DB_LOCK, SNAPSHOT_LOCAL_DATA_LOCK}),
  // Bootstrap lock for OM. (Bootstrap lock should be acquired before any other lock).
  BOOTSTRAP_LOCK("BOOTSTRAP_LOCK",
      new DAGLeveledResource[] {SNAPSHOT_GC_LOCK, SNAPSHOT_DB_LOCK, SNAPSHOT_DB_CONTENT_LOCK,
          SNAPSHOT_LOCAL_DATA_LOCK});

  private Set<DAGLeveledResource> children;
  private String name;
  private IOzoneManagerLock.ResourceManager resourceManager;

  DAGLeveledResource(String name) {
    this(name, null);
  }

  DAGLeveledResource(String name, DAGLeveledResource[] children) {
    this.name = name;
    this.resourceManager = new IOzoneManagerLock.ResourceManager();
    // Filter out this resource from the children set to avoid self dependencies.
    this.children = children == null ? Collections.emptySet() : Collections.unmodifiableSet(
        Arrays.stream(children).filter(i -> i != this).collect(Collectors.toSet()));
  }

  @Override
  public String getName() {
    return name;
  }

  public Set<DAGLeveledResource> getChildren() {
    return children;
  }

  @Override
  public IOzoneManagerLock.ResourceManager getResourceManager() {
    return resourceManager;
  }
}
