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

  @JsonProperty("volume")
  private String volume;

  public VolumeObjectDBInfo() {

  }

  public VolumeObjectDBInfo(OmVolumeArgs omVolumeArgs) {
    super.setMetadata(omVolumeArgs.getMetadata());
    super.setName(omVolumeArgs.getVolume());
    super.setQuotaInBytes(omVolumeArgs.getQuotaInBytes());
    super.setQuotaInNamespace(omVolumeArgs.getQuotaInNamespace());
    super.setUsedNamespace(omVolumeArgs.getUsedNamespace());
    super.setCreationTime(omVolumeArgs.getCreationTime());
    super.setModificationTime(omVolumeArgs.getModificationTime());
    super.setAcls(AclMetadata.fromOzoneAcls(omVolumeArgs.getAcls()));
    this.setAdmin(omVolumeArgs.getAdminName());
    this.setOwner(omVolumeArgs.getOwnerName());
    this.setVolume(omVolumeArgs.getVolume());
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

  public String getVolume() {
    return volume;
  }

  public void setVolume(String volume) {
    this.volume = volume;
  }
}
