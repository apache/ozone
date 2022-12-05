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

import java.util.List;
import java.util.Map;

/**
 * Encapsulates the low level volume/bucket/dir info.
 */
public class ObjectDBInfo {
  /** object metadata from om db. */
  @JsonProperty("metadata")
  private Map<String, String> metadata;

  /** name of the object. */
  @JsonProperty("name")
  private String name;

  /** quota in bytes. */
  @JsonProperty("quotaInBytes")
  private long quotaInBytes;

  /** quota in namespace. */
  @JsonProperty("quotaInNamespace")
  private long quotaInNamespace;

  /** used namespace. */
  @JsonProperty("usedNamespace")
  private long usedNamespace;

  /** creation time. */
  @JsonProperty("creationTime")
  private long creationTime;

  /** modification time. */
  @JsonProperty("modificationTime")
  private long modificationTime;

  /** acl list. */
  @JsonProperty("acls")
  private List<OzoneAcl> acls;

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
}
