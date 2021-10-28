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

import org.apache.hadoop.hdds.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used for storing info related to the Kerberos principal.
 *
 * For now this is merely used to store a list of accessIds associates with the
 * principal, but can be extended to store more fields later.
 */
public final class OmDBKerberosPrincipalInfo {

  /**
   * A set of accessIds.
   */
  private final Set<String> accessIds;

  // This implies above String fields should NOT contain the split key.
  // TODO: Reject user input accessId if it contains the split key.
  public static final String SERIALIZATION_SPLIT_KEY = ";";

  public OmDBKerberosPrincipalInfo(Set<String> accessIds) {
    this.accessIds = new HashSet<>(accessIds);
  }

  private OmDBKerberosPrincipalInfo(String serialized) {
    accessIds = Arrays.stream(serialized.split(SERIALIZATION_SPLIT_KEY))
        // Remove any empty accessId strings when deserializing
        .filter(e -> !e.isEmpty()).collect(Collectors.toSet());
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

  private String serialize() {
    return String.join(SERIALIZATION_SPLIT_KEY, accessIds);
  }

  /**
   * Convert OmDBKerberosPrincipalInfo to byteArray to be persisted to DB.
   * @return byte[]
   */
  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(serialize());
  }

  /**
   * Convert byte array to OmDBKerberosPrincipalInfo.
   */
  public static OmDBKerberosPrincipalInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new OmDBKerberosPrincipalInfo(tInfo);
  }

  /**
   * Builder for OmDBKerberosPrincipalInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private Set<String> accessIds;

    public Builder setAccessIds(Set<String> accessIds) {
      this.accessIds = accessIds;
      return this;
    }

    public OmDBKerberosPrincipalInfo build() {
      return new OmDBKerberosPrincipalInfo(accessIds);
    }
  }
}
