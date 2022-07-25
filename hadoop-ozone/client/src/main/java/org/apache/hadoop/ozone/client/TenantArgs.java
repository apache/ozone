/**
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

package org.apache.hadoop.ozone.client;

import com.google.common.base.Preconditions;

/**
 * This class encapsulates the arguments for creating a tenant.
 */
public final class TenantArgs {

  /**
   * Name of the volume to be created for the tenant.
   */
  private final String volumeName;

  /**
   * Private constructor, constructed via builder.
   * @param volumeName Volume name.
   */
  private TenantArgs(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  /**
   * Returns new builder class that builds a TenantArgs.
   *
   * @return Builder
   */
  public static TenantArgs.Builder newBuilder() {
    return new TenantArgs.Builder();
  }

  /**
   * Builder for TenantArgs.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static class Builder {
    private String volumeName;

    /**
     * Constructs a builder.
     */
    public Builder() {
    }

    public TenantArgs.Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    /**
     * Constructs a TenantArgs.
     * @return TenantArgs.
     */
    public TenantArgs build() {
      Preconditions.checkNotNull(volumeName);
      return new TenantArgs(volumeName);
    }
  }

}
