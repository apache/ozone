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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;

/**
 * Encapsulates the low level DB info common to volume or bucket or dir.
 */
public class ObjectDBInfo {
  @JsonProperty("metadata")
  private Map<String, String> metadata;

  @JsonProperty("name")
  private String name;

  @JsonProperty("quotaInBytes")
  private long quotaInBytes;

  @JsonProperty("quotaInNamespace")
  private long quotaInNamespace;

  @JsonProperty("usedNamespace")
  private long usedNamespace;

  @JsonProperty("creationTime")
  private long creationTime;

  @JsonProperty("modificationTime")
  private long modificationTime;

  @JsonProperty("acls")
  private List<AclMetadata> acls;

  public ObjectDBInfo() {

  }

  public ObjectDBInfo(OmDirectoryInfo omDirectoryInfo) {
    this.setName(omDirectoryInfo.getName());
    this.setCreationTime(omDirectoryInfo.getCreationTime());
    this.setModificationTime(omDirectoryInfo.getModificationTime());
    this.setAcls(AclMetadata.fromOzoneAcls(omDirectoryInfo.getAcls()));
    this.setMetadata(omDirectoryInfo.getMetadata());
  }

  public ObjectDBInfo(OmPrefixInfo omPrefixInfo) {
    this.setName(omPrefixInfo.getName());
    this.setAcls(AclMetadata.fromOzoneAcls(omPrefixInfo.getAcls()));
    this.setMetadata(omPrefixInfo.getMetadata());
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public void setQuotaInBytes(long quotaInBytes) {
    this.quotaInBytes = quotaInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public void setQuotaInNamespace(long quotaInNamespace) {
    this.quotaInNamespace = quotaInNamespace;
  }

  public long getUsedNamespace() {
    return usedNamespace;
  }

  public void setUsedNamespace(long usedNamespace) {
    this.usedNamespace = usedNamespace;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  public List<AclMetadata> getAcls() {
    return acls;
  }

  public void setAcls(List<AclMetadata> acls) {
    this.acls = acls;
  }

}
