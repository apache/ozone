/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.hdds.fs.CachingSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckParams;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;

/**
 * Class that wraps the space df of the Datanode Volumes used by SCM
 * containers.
 */
public class VolumeUsage implements SpaceUsageSource {

  private final CachingSpaceUsageSource source;
  private boolean shutdownComplete;

  VolumeUsage(SpaceUsageCheckParams checkParams) {
    source = new CachingSpaceUsageSource(checkParams);
    start(); // TODO should start only on demand
  }

  @Override
  public long getCapacity() {
    return Math.max(source.getCapacity(), 0);
  }

  @Override
  public long getAvailable() {
    long l = source.getCapacity() - source.getUsedSpace();
    return Math.max(Math.min(l, source.getAvailable()), 0);
  }

  @Override
  public long getUsedSpace() {
    return source.getUsedSpace();
  }

  public synchronized void start() {
    source.start();
  }

  public synchronized void shutdown() {
    if (!shutdownComplete) {
      source.shutdown();
      shutdownComplete = true;
    }
  }

}
