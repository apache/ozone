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

package org.apache.hadoop.ozone.container.common.interfaces;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;

/**
 * This interface specifies the policy for choosing volumes to store replicas.
 */
@InterfaceAudience.Private
public interface VolumeChoosingPolicy {

  /**
   * Choose a volume to place a container, optionally constraining the choice
   * to a specific storage type.
   *
   * @param volumes a list of available volumes.
   * @param maxContainerSize the maximum size of the container for which a
   *                         volume is sought.
   * @param storageType the requested storage type, or {@code null} to allow
   *                    the policy to choose from any volume.
   * @return the chosen volume.
   * @throws IOException when disks are unavailable or are full.
   */
  HddsVolume chooseVolume(List<HddsVolume> volumes, long maxContainerSize,
      @Nullable StorageType storageType) throws IOException;
}
