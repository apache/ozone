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

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserAccessId;

/**
 * Class to encapsulate the list of users and corresponding accessIds
 * associated with a tenant.
 */
public class TenantUserList {

  private final String tenantName;

  private final List<TenantUserAccessId> userAccessIds;


  public TenantUserList(String tenantName,
                        List<TenantUserAccessId> userAccessIds) {
    this.tenantName = tenantName;
    this.userAccessIds = userAccessIds;
  }

  public String getTenantName() {
    return tenantName;
  }

  public List<TenantUserAccessId> getUserAccessIds() {
    return userAccessIds;
  }

  public static TenantUserList fromProtobuf(TenantListUserResponse response) {
    return new TenantUserList(response.getTenantName(),
        response.getUserAccessIdInfoList());
  }

  @Override
  public String toString() {
    return "tenantName=" + tenantName +
        "\nuserAccessIds=" + userAccessIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TenantUserList that = (TenantUserList) o;
    return tenantName.equals(that.tenantName) &&
        userAccessIds.equals(that.userAccessIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, userAccessIds);
  }
}
