/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is used for storing info related to the Kerberos principal.
 *
 * For now this only stores a list of accessIds associates with the user
 * principal.
 */
public final class OmDBUserPrincipalInfo {

  /**
   * A set of accessIds.
   */
  private final Set<String> accessIds;

  public OmDBUserPrincipalInfo(Set<String> accessIds) {
    this.accessIds = new HashSet<>(accessIds);
  }

  public Set<String> getAccessIds() {
    return accessIds;
  }

  public boolean addAccessId(String accessId) {
    return accessIds.add(accessId);
  }

  public boolean removeAccessId(String accessId) {
    return accessIds.remove(accessId);
  }

  public boolean hasAccessId(String accessId) {
    return accessIds.contains(accessId);
  }

  /**
   * Convert OmDBUserPrincipalInfo to protobuf to be persisted to DB.
   */
  public OzoneManagerProtocolProtos.TenantUserPrincipalInfo getProtobuf() {
    return OzoneManagerProtocolProtos.TenantUserPrincipalInfo.newBuilder()
        .addAllAccessIds(accessIds)
        .build();
  }

  /**
   * Convert protobuf to OmDBUserPrincipalInfo.
   */
  public static OmDBUserPrincipalInfo getFromProtobuf(
      OzoneManagerProtocolProtos.TenantUserPrincipalInfo proto) {
    return new Builder()
        .setAccessIds(new HashSet<>(proto.getAccessIdsList()))
        .build();
  }

  /**
   * Builder for OmDBUserPrincipalInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private Set<String> accessIds;

    public Builder setAccessIds(Set<String> accessIds) {
      this.accessIds = accessIds;
      return this;
    }

    public OmDBUserPrincipalInfo build() {
      return new OmDBUserPrincipalInfo(accessIds);
    }
  }
}
