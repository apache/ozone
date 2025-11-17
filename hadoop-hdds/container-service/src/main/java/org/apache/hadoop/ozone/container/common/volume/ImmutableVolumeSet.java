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

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Fixed list of volumes.
 */
public final class ImmutableVolumeSet implements VolumeSet {

  private final List<StorageVolume> volumes;

  public ImmutableVolumeSet(StorageVolume... volumes) {
    this.volumes = ImmutableList.copyOf(volumes);
  }

  public ImmutableVolumeSet(Collection<? extends StorageVolume> volumes) {
    this.volumes = ImmutableList.copyOf(volumes);
  }

  @Override
  public List<StorageVolume> getVolumesList() {
    return volumes;
  }

  @Override
  public void checkAllVolumes(StorageVolumeChecker checker) throws IOException {
    try {
      checker.checkAllVolumes(volumes);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while running disk check", e);
    }
  }

  @Override
  public void readLock() {
    // no-op, immutable
  }

  @Override
  public void readUnlock() {
    // no-op, immutable
  }

  @Override
  public void writeLock() {
    // no-op, immutable
  }

  @Override
  public void writeUnlock() {
    // no-op, immutable
  }
}
