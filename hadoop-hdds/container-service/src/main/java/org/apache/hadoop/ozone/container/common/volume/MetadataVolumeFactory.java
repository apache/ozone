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
 * A factory class for MetadataVolume.
 */
public class MetadataVolumeFactory extends StorageVolumeFactory {

  public MetadataVolumeFactory(ConfigurationSource conf,
      SpaceUsageCheckFactory usageCheckFactory, MutableVolumeSet volumeSet) {
    super(conf, usageCheckFactory, volumeSet, null, null);
  }

  @Override
  StorageVolume createVolume(String locationString, StorageType storageType)
      throws IOException {
    MetadataVolume.Builder volumeBuilder =
        new MetadataVolume.Builder(locationString)
            .conf(getConf())
            .usageCheckFactory(getUsageCheckFactory())
            .storageType(storageType)
            .volumeSet(getVolumeSet());
    return volumeBuilder.build();
  }

  @Override
  StorageVolume createFailedVolume(String locationString) throws IOException {
    MetadataVolume.Builder volumeBuilder =
        new MetadataVolume.Builder(locationString)
            .failedVolume(true);
    return volumeBuilder.build();
  }
}
