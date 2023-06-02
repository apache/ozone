/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.fs.StorageType.SSD;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Policy to choose [SSD] labeled volume.
 * And fallback to the remaining available volumes if no applicable SSD
 * was found
 */
public class SsdAndRemainingVolumesFallbackChoosingPolicy
    extends SsdVolumeChoosingPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(
      SsdAndRemainingVolumesFallbackChoosingPolicy.class);

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
                                 long maxContainerSize) throws IOException {
    try {
      return super.chooseVolume(volumes, maxContainerSize);
    } catch (DiskChecker.DiskOutOfSpaceException ex) {
      LOG.warn("Applicable SSD volume wasn't found, falling back to other"
          + " available volumes");
      return getDelegate().chooseVolume(volumes.stream()
          .filter(vol -> !vol.getStorageType().equals(SSD))
          .collect(Collectors.toList()), maxContainerSize);
    }
  }
}
