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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;

/**
 * A factory class for HddsVolume.
 */
public class HddsVolumeFactory extends StorageVolumeFactory {

  public HddsVolumeFactory(ConfigurationSource conf,
      SpaceUsageCheckFactory usageCheckFactory, MutableVolumeSet volumeSet,
      String datanodeUuid, String clusterID) {
    super(conf, usageCheckFactory, volumeSet, datanodeUuid, clusterID);
  }

  @Override
  public StorageVolume createVolume(String locationString,
      StorageType storageType) throws IOException {
    HddsVolume.Builder volumeBuilder = new HddsVolume.Builder(locationString)
        .conf(getConf())
        .datanodeUuid(getDatanodeUuid())
        .clusterID(getClusterID())
        .usageCheckFactory(getUsageCheckFactory())
        .storageType(storageType)
        .volumeSet(getVolumeSet());
    HddsVolume volume = volumeBuilder.build();

    checkAndSetClusterID(volume.getClusterID());

    return volume;
  }

  @Override
  public StorageVolume createFailedVolume(String locationString)
      throws IOException {
    HddsVolume.Builder volumeBuilder = new HddsVolume.Builder(locationString)
        .failedVolume(true);
    return volumeBuilder.build();
  }
}
