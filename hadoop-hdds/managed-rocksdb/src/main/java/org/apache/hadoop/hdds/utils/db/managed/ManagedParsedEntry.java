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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.ParsedEntryInfo;

/**
 * ManagedParsedEntry is a subclass of ParsedEntryInfo that ensures proper
 * tracking and closure of resources to prevent resource leakage. This class
 * leverages an internal leak tracker to monitor lifecycle events and ensures
 * that native resources are released correctly when the object is closed.
 *
 * It overrides the {@code close} method to integrate the cleanup logic for
 * resource tracking, delegating the resource closure to its parent class, and
 * subsequently ensuring the associated leak tracker is closed as well.
 */
public class ManagedParsedEntry extends ParsedEntryInfo {
  private final UncheckedAutoCloseable leakTracker = track(this);

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      leakTracker.close();
    }
  }
}
