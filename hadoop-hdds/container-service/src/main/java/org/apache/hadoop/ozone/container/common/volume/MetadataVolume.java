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

/**
 * MetadataVolume represents a volume in datanode for metadata(ratis).
 * Datanode itself doesn't consume this volume, but only manages checks
 * and volume info for it.
 */
public class MetadataVolume extends StorageVolume {

  private final VolumeType type = VolumeType.META_VOLUME;

  protected MetadataVolume(Builder b) throws IOException {
    super(b);
    // Tmp directory on Metadata Volume uses the volume root as the working
    // directory. It is not dependent on the cluster ID from SCM.
    super.createTmpDirs("");
  }

  public VolumeType getType() {
    return type;
  }

  @Override
  public void format(String cid) throws IOException {
    // No-op for Metadata volumes.
  }

  @Override
  public void createTmpDirs(String workingDirName) {
    // No-op for metadata volumes.
    // Tmp directory is created on construction since it is not dependent on
    // getting the cluster ID from SCM.
  }

  /**
   * Builder class for MetadataVolume.
   */
  public static class Builder extends StorageVolume.Builder<Builder> {

    public Builder(String volumeRootStr) {
      super(volumeRootStr, "");
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public MetadataVolume build() throws IOException {
      return new MetadataVolume(this);
    }
  }

  @Override
  public String getStorageID() {
    return "";
  }
}
