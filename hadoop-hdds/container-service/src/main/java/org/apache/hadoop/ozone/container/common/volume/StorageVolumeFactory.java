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
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;

/**
 * A factory class for StorageVolume.
 */
public abstract class StorageVolumeFactory {

  private ConfigurationSource conf;
  private SpaceUsageCheckFactory usageCheckFactory;
  private MutableVolumeSet volumeSet;
  private String datanodeUuid;
  private String clusterID;

  public StorageVolumeFactory(ConfigurationSource conf,
      SpaceUsageCheckFactory usageCheckFactory, MutableVolumeSet volumeSet,
      String datanodeUuid, String clusterID) {
    this.conf = conf;
    this.usageCheckFactory = usageCheckFactory;
    this.volumeSet = volumeSet;
    this.datanodeUuid = datanodeUuid;
    this.clusterID = clusterID;
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

  public String getDatanodeUuid() {
    return this.datanodeUuid;
  }

  public String getClusterID() {
    return this.clusterID;
  }

  /**
   * If Version file exists and the {@link #clusterID} is not set yet,
   * assign it the value from Version file. Otherwise, check that the given
   * id matches with the id from version file.
   * @param idFromVersionFile value of the property from Version file
   * @throws InconsistentStorageStateException
   */
  protected void checkAndSetClusterID(String idFromVersionFile)
      throws InconsistentStorageStateException {
    // If the clusterID is null (not set), assign it the value
    // from version file.
    if (this.clusterID == null) {
      this.clusterID = idFromVersionFile;
      return;
    }

    // If the clusterID is already set, it should match with the value from the
    // version file.
    if (!idFromVersionFile.equals(this.clusterID)) {
      throw new InconsistentStorageStateException(
          "Mismatched ClusterIDs. VolumeSet has: " + this.clusterID +
              ", and version file has: " + idFromVersionFile);
    }
  }

  abstract StorageVolume createVolume(String locationString,
      StorageType storageType) throws IOException;

  abstract StorageVolume createFailedVolume(String locationString)
      throws IOException;
}
