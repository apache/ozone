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
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates the low level volume/bucket/dir info.
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
  private List<OzoneAcl> acls;

  public static ObjectDBInfo.PrefixObjectDbInfoBuilder
      newPrefixObjectDbInfoBuilder() {
    return new ObjectDBInfo.PrefixObjectDbInfoBuilder();
  }

  public ObjectDBInfo(PrefixObjectDbInfoBuilder
                          prefixObjectDbInfoBuilder) {
    this.setMetadata(prefixObjectDbInfoBuilder.
        getOmPrefixInfo().getMetadata());
    this.setName(prefixObjectDbInfoBuilder.
        getOmPrefixInfo().getName());
    this.setAcls(prefixObjectDbInfoBuilder.
        getOmPrefixInfo().getAcls());
  }

  public static ObjectDBInfo.DirObjectDbInfoBuilder
      newDirObjectDbInfoBuilder() {
    return new ObjectDBInfo.DirObjectDbInfoBuilder();
  }

  public ObjectDBInfo() {

  }

  public ObjectDBInfo(DirObjectDbInfoBuilder b) {
    this.setName(b.getDirInfo().getName());
    this.setCreationTime(b.getDirInfo().getCreationTime());
    this.setModificationTime(b.getDirInfo().getModificationTime());
    this.setAcls(b.getDirInfo().getAcls());
    this.setMetadata(b.getDirInfo().getMetadata());
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

  public List<OzoneAcl> getAcls() {
    return acls;
  }

  public void setAcls(List<OzoneAcl> acls) {
    this.acls = acls;
  }

  /**
   * Builder for Directory ObjectDBInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class DirObjectDbInfoBuilder {
    private OmDirectoryInfo dirInfo;

    public DirObjectDbInfoBuilder() {

    }

    public DirObjectDbInfoBuilder setDirInfo(
        OmDirectoryInfo dirInfo) {
      this.dirInfo = dirInfo;
      return this;
    }

    public OmDirectoryInfo getDirInfo() {
      return dirInfo;
    }

    public ObjectDBInfo build() {
      if (null == this.dirInfo) {
        return new ObjectDBInfo();
      }
      return new ObjectDBInfo(this);
    }
  }

  /**
   * Builder for Prefix ObjectDBInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class PrefixObjectDbInfoBuilder {
    private OmPrefixInfo omPrefixInfo;

    public PrefixObjectDbInfoBuilder() {

    }

    public PrefixObjectDbInfoBuilder setOmPrefixInfo(
        OmPrefixInfo omPrefixInfo) {
      this.omPrefixInfo = omPrefixInfo;
      return this;
    }

    public OmPrefixInfo getOmPrefixInfo() {
      return omPrefixInfo;
    }

    public ObjectDBInfo build() {
      if (null == this.omPrefixInfo) {
        return new ObjectDBInfo();
      }
      return new ObjectDBInfo(this);
    }
  }
}
