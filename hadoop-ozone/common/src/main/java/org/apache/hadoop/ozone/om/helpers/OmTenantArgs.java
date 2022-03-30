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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;

/**
 * This class is used for storing Ozone tenant arguments.
 */
public class OmTenantArgs {

  /**
   * Tenant name.
   */
  private final String tenantId;

  /**
   * Volume name to be created for this tenant.
   * Default volume name would be the same as tenant name if unspecified.
   */
  private final String volumeName;

  public OmTenantArgs(String tenantId) {
    this.tenantId = tenantId;
    this.volumeName = this.tenantId;
  }

  public OmTenantArgs(String tenantId, String volumeName) {
    this.tenantId = tenantId;
    this.volumeName = volumeName;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public static OmTenantArgs.Builder newBuilder() {
    return new OmTenantArgs.Builder();
  }

  /**
   * Builder for OmTenantArgs.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static class Builder {
    private String tenantId;
    private String volumeName;

    /**
     * Constructs a builder.
     */
    public Builder() {
    }

    public Builder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public OmTenantArgs build() {
      Preconditions.checkNotNull(tenantId);
      Preconditions.checkNotNull(volumeName);
      return new OmTenantArgs(tenantId, volumeName);
    }
  }

}
