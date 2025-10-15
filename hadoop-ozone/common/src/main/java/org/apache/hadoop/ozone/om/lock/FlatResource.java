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

import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock.Resource;

/**
 * Flat Resource defined in Ozone. Locks can be acquired on a resource independent of one another.
 */
public enum FlatResource implements Resource {
  // Background services lock on a Snapshot.
  SNAPSHOT_GC_LOCK("SNAPSHOT_GC_LOCK"),
  // Lock acquired on a Snapshot's RocksDB Handle.
  SNAPSHOT_DB_LOCK("SNAPSHOT_DB_LOCK");

  private String name;
  private IOzoneManagerLock.ResourceManager resourceManager;

  FlatResource(String name) {
    this.name = name;
    this.resourceManager = new IOzoneManagerLock.ResourceManager();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public IOzoneManagerLock.ResourceManager getResourceManager() {
    return resourceManager;
  }
}
