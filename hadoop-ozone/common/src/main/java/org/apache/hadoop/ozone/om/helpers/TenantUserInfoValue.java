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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExtendedAccessIdInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoResponse;

import java.util.List;
import java.util.Objects;

/**
 * Utility class to handle protobuf message TenantUserInfo conversion.
 */
public class TenantUserInfoValue {

  // Usually this is the Kerberos principal of a user.
  private final String userPrincipal;

  // A list of ExtendedAccessIdInfo from protobuf.
  private final List<ExtendedAccessIdInfo> accessIdInfoList;

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public List<ExtendedAccessIdInfo> getAccessIdInfoList() {
    return accessIdInfoList;
  }

  public TenantUserInfoValue(String kerberosID,
      List<ExtendedAccessIdInfo> accessIdInfoList) {
    this.userPrincipal = kerberosID;
    this.accessIdInfoList = accessIdInfoList;
  }

  public static TenantUserInfoValue fromProtobuf(
      TenantGetUserInfoResponse tenantUserInfo) {
    return new TenantUserInfoValue(tenantUserInfo.getUserPrincipal(),
        tenantUserInfo.getAccessIdInfoList());
  }

  public TenantGetUserInfoResponse getProtobuf() {
    final TenantGetUserInfoResponse.Builder builder =
        TenantGetUserInfoResponse.newBuilder();
    builder.setUserPrincipal(this.userPrincipal);
    accessIdInfoList.forEach(builder::addAccessIdInfo);
    return builder.build();
  }

  @Override
  public String toString() {
    return "userPrincipal=" + userPrincipal +
        "\naccessIdInfoList=" + accessIdInfoList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TenantUserInfoValue that = (TenantUserInfoValue) o;
    return userPrincipal.equals(that.userPrincipal) &&
        accessIdInfoList.equals(that.accessIdInfoList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userPrincipal, accessIdInfoList);
  }
}
