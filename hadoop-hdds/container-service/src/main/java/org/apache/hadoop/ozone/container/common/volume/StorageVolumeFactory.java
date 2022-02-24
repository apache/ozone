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

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;

import java.io.IOException;

/**
 * A factory class for StorageVolume.
 */
public abstract class StorageVolumeFactory {

  private ConfigurationSource conf;
  private SpaceUsageCheckFactory usageCheckFactory;
  private MutableVolumeSet volumeSet;

  public StorageVolumeFactory(ConfigurationSource conf,
      SpaceUsageCheckFactory usageCheckFactory, MutableVolumeSet volumeSet) {
    this.conf = conf;
    this.usageCheckFactory = usageCheckFactory;
    this.volumeSet = volumeSet;
  }

  public ConfigurationSource getConf() {
    return conf;
  }

  public SpaceUsageCheckFactory getUsageCheckFactory() {
    return usageCheckFactory;
  }

  public VolumeSet getVolumeSet() {
    return this.volumeSet;
  }

  abstract StorageVolume createVolume(String locationString,
      StorageType storageType) throws IOException;

  abstract StorageVolume createFailedVolume(String locationString)
      throws IOException;
}
