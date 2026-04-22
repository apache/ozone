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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;

/**
 * Base volume choosing policy with optional storage-type filtering.
 */
public abstract class AbstractStorageTypeChoosingPolicy
    implements VolumeChoosingPolicy {

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize) throws IOException {
    return chooseVolume(volumes, maxContainerSize, null);
  }

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
      long maxContainerSize, StorageType storageType) throws IOException {
    List<HddsVolume> candidates = volumes;
    if (storageType != null) {
      candidates = volumes.stream()
          .filter(volume -> volume.getStorageType() == storageType)
          .collect(Collectors.toList());
    }
    return chooseVolumeInternal(candidates, maxContainerSize);
  }

  protected abstract HddsVolume chooseVolumeInternal(List<HddsVolume> volumes,
      long maxContainerSize) throws IOException;
}
