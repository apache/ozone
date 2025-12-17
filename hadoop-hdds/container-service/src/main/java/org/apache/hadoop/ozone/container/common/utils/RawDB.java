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

package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;

/**
 * Just a wrapper for DatanodeStore.
 * This is for container schema v3 which has one rocksdb instance per disk.
 */
public class RawDB extends DBHandle {

  public RawDB(DatanodeStore store, String containerDBPath) {
    super(store, containerDBPath);
  }

  @Override
  public void close() {
    // NOTE: intend to do nothing on close
    // With schema v3, block operations on a single container should not
    // close the whole db handle.
    // Will close the low-level stores all together in a collection class.
  }
}
