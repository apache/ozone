/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;

/**
 * Encapsulates the low level volume info.
 */
public class VolumeObjectDBInfo extends ObjectDBInfo {
  @JsonProperty("admin")
  private String admin;

  @JsonProperty("owner")
  private String owner;

  public static VolumeObjectDBInfo.Builder newBuilder() {
    return new VolumeObjectDBInfo.Builder();
  }

  public VolumeObjectDBInfo() {

  }

  public VolumeObjectDBInfo(Builder b) {
    this.setMetadata(b.getVolumeArgs().getMetadata());
    this.setName(b.getVolumeArgs().getVolume());
    this.setAdmin(b.getVolumeArgs().getAdminName());
    this.setOwner(b.getVolumeArgs().getOwnerName());
    this.setQuotaInBytes(b.getVolumeArgs().getQuotaInBytes());
    this.setQuotaInNamespace(b.getVolumeArgs().getQuotaInNamespace());
    this.setUsedNamespace(b.getVolumeArgs().getUsedNamespace());
    this.setCreationTime(b.getVolumeArgs().getCreationTime());
    this.setModificationTime(b.getVolumeArgs().getModificationTime());
    this.setAcls(b.getVolumeArgs().getAcls());
  }

  public String getAdmin() {
    return admin;
  }

  public void setAdmin(String admin) {
    this.admin = admin;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * Builder for VolumeObjectDBInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private OmVolumeArgs volumeArgs;
    public Builder() {

    }

    public VolumeObjectDBInfo.Builder setVolumeArgs(
        OmVolumeArgs volumeArgs) {
      this.volumeArgs = volumeArgs;
      return this;
    }

    public OmVolumeArgs getVolumeArgs() {
      return volumeArgs;
    }

    public VolumeObjectDBInfo build() {
      if (null == this.volumeArgs) {
        return new VolumeObjectDBInfo();
      }
      return new VolumeObjectDBInfo(this);
    }
  }
}
