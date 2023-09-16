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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneAcl;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Metadata object represents one Ozone ACL.
 */
public final class AclMetadata {

  @JsonProperty("type")
  private String type;
  @JsonProperty("name")
  private String name;
  @JsonProperty("scope")
  private String scope;
  @JsonProperty("aclList")
  private List<String> aclList;

  private AclMetadata(Builder builder) {
    this.type = builder.type;
    this.name = builder.name;
    this.scope = builder.scope;
    this.aclList = builder.aclList;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getScope() {
    return scope;
  }

  public List<String> getAclList() {
    return aclList;
  }

  /**
   * Returns new builder class that builds a AclMetadata.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for AclMetadata.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String type;
    private String name;
    private String scope;
    private List<String> aclList;

    public Builder() {

    }

    public Builder withType(String type) {
      this.type = type;
      return this;
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withScope(String scope) {
      this.scope = scope;
      return this;

    }

    public Builder withAclList(List<String> aclList) {
      this.aclList = aclList;
      return this;
    }

    public AclMetadata build() {
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(scope);

      return new AclMetadata(this);
    }

  }

  public static List<AclMetadata> fromOzoneAcls(List<OzoneAcl> ozoneAcls) {
    return ozoneAcls.stream().map(AclMetadata::fromOzoneAcl)
        .collect(Collectors.toList());
  }

  public static AclMetadata fromOzoneAcl(OzoneAcl ozoneAcl) {
    if (ozoneAcl == null) {
      return null;
    }

    AclMetadata.Builder builder = AclMetadata.newBuilder();

    return builder.withType(ozoneAcl.getType().toString().toUpperCase())
        .withName(ozoneAcl.getName())
        .withScope(ozoneAcl.getAclScope().toString().toUpperCase())
        .withAclList(ozoneAcl.getAclList().stream().map(Enum::toString)
            .map(String::toUpperCase)
            .collect(Collectors.toList()))
        .build();
  }
}
