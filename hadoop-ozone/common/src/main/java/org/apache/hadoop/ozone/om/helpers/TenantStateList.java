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

package org.apache.hadoop.ozone.om.helpers;

import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantState;

/**
 * Utility class to handle protobuf message TenantState conversion.
 */
public class TenantStateList {

  // A list of TenantState from protobuf.
  private final List<TenantState> tenantStateList;

  public List<TenantState> getTenantStateList() {
    return tenantStateList;
  }

  public TenantStateList(List<TenantState> tenantStateList) {
    this.tenantStateList = tenantStateList;
  }

  public static TenantStateList fromProtobuf(
      List<TenantState> tenantStateList) {
    return new TenantStateList(tenantStateList);
  }

  public TenantState getProtobuf() {
    throw new NotImplementedException("getProtobuf() not implemented");
  }

  @Override
  public String toString() {
    return "tenantStateList=" + tenantStateList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TenantStateList that = (TenantStateList) o;
    return tenantStateList.equals(that.tenantStateList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantStateList);
  }
}
