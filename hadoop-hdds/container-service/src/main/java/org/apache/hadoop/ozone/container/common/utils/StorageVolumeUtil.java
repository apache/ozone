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

package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.ozone.container.common.volume.DbVolume;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A util class for {@link StorageVolume}.
 */
public final class StorageVolumeUtil {

  private StorageVolumeUtil() {
  }

  public static void onFailure(StorageVolume volume) {
    if (volume != null) {
      VolumeSet volumeSet = volume.getVolumeSet();
      if (volumeSet != null && volumeSet instanceof MutableVolumeSet) {
        ((MutableVolumeSet) volumeSet).checkVolumeAsync(volume);
      }
    }
  }

  public static List<HddsVolume> getHddsVolumesList(
      List<StorageVolume> volumes) {
    return volumes.stream().
        map(v -> (HddsVolume) v).collect(Collectors.toList());
  }

  public static List<DbVolume> getDbVolumesList(
      List<StorageVolume> volumes) {
    return volumes.stream().
        map(v -> (DbVolume) v).collect(Collectors.toList());
  }
}
