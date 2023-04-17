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
package org.apache.hadoop.ozone.recon.api.types;

import com.google.common.base.Preconditions;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.Instant;
import java.util.List;

/**
 * Metadata object represents one volume in the object store.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public final class VolumeMetadata {

  @XmlElement(name = "volume")
  private String volume;
  @XmlElement(name = "owner")
  private String owner;
  @XmlElement(name = "admin")
  private String admin;
  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "creationTime")
  private Instant creationTime;
  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "modificationTime")
  private Instant modificationTime;
  @XmlElement(name = "quotaInBytes")
  private long quotaInBytes;
  @XmlElement(name = "quotaInNamespace")
  private long quotaInNamespace;
  @XmlElement(name = "usedNamespace")
  private long usedNamespace;
  @XmlElement(name = "acls")
  private List<AclMetadata> acls;

  private VolumeMetadata(Builder builder) {
    this.volume = builder.volume;
    this.owner = builder.owner;
    this.admin = builder.admin;
    this.creationTime = builder.creationTime;
    this.modificationTime = builder.modificationTime;
    this.quotaInBytes = builder.quotaInBytes;
    this.quotaInNamespace = builder.quotaInNamespace;
    this.usedNamespace = builder.usedNamespace;
    this.acls = builder.acls;
  }

  public String getVolume() {
    return volume;
  }

  public String getOwner() {
    return owner;
  }

  public String getAdmin() {
    return admin;
  }

  public Instant getCreationTime() {
    return creationTime;
  }

  public Instant getModificationTime() {
    return modificationTime;
  }

  public long getQuotaInBytes() {
    return quotaInBytes;
  }

  public long getQuotaInNamespace() {
    return quotaInNamespace;
  }

  public long getUsedNamespace() {
    return usedNamespace;
  }

  public List<AclMetadata> getAcls() {
    return acls;
  }

  /**
   * Returns new builder class that builds a VolumeMetadata.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for VolumeMetadata.
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public static final class Builder {
    private String volume;
    private String owner;
    private String admin;
    private Instant creationTime;
    private Instant modificationTime;
    private long quotaInBytes;
    private long quotaInNamespace;
    private long usedNamespace;
    private List<AclMetadata> acls;

    public Builder() {

    }

    public Builder withVolume(String volume) {
      this.volume = volume;
      return this;
    }

    public Builder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder withAdmin(String admin) {
      this.admin = admin;
      return this;
    }

    public Builder withCreationTime(Instant creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    public Builder withModificationTime(Instant modificationTime) {
      this.modificationTime = modificationTime;
      return this;
    }

    public Builder withQuotaInBytes(long quotaInBytes) {
      this.quotaInBytes = quotaInBytes;
      return this;
    }

    public Builder withQuotaInNamespace(long quotaInNamespace) {
      this.quotaInNamespace = quotaInNamespace;
      return this;
    }

    public Builder withUsedNamespace(long usedNamespace) {
      this.usedNamespace = usedNamespace;
      return this;
    }

    public Builder withAcls(List<AclMetadata> acls) {
      this.acls  = acls;
      return this;
    }

    /**
     * Constructs VolumeMetadata.
     *
     * @return instance of VolumeMetadata
     */
    public VolumeMetadata build() {
      Preconditions.checkNotNull(volume);

      return new VolumeMetadata(this);
    }
  }
}
