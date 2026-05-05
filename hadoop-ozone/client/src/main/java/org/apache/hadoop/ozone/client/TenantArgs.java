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

package org.apache.hadoop.ozone.client;

import java.util.Objects;

/**
 * This class encapsulates the arguments for creating a tenant.
 */
public final class TenantArgs {

  /**
   * Name of the volume to be created for the tenant.
   */
  private final String volumeName;

  /**
   * Force tenant creation when volume exists.
   */
  private final boolean forceCreationWhenVolumeExists;

  /**
   * Private constructor, constructed via builder.
   *
   * @param volumeName                    Volume name.
   * @param forceCreationWhenVolumeExists Force creation when volume exists.
   */
  private TenantArgs(String volumeName, boolean forceCreationWhenVolumeExists) {
    this.volumeName = volumeName;
    this.forceCreationWhenVolumeExists = forceCreationWhenVolumeExists;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public boolean getForceCreationWhenVolumeExists() {
    return forceCreationWhenVolumeExists;
  }

  /**
   * Returns new builder class that builds a TenantArgs.
   *
   * @return Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for TenantArgs.
   */
  public static class Builder {
    private String volumeName;
    private boolean forceCreationWhenVolumeExists;

    /**
     * Constructs a builder.
     */
    public Builder() {
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setForceCreationWhenVolumeExists(
        boolean forceCreationWhenVolumeExists) {
      this.forceCreationWhenVolumeExists = forceCreationWhenVolumeExists;
      return this;
    }

    /**
     * Constructs a TenantArgs.
     * @return TenantArgs.
     */
    public TenantArgs build() {
      Objects.requireNonNull(volumeName, "volumeName == null");
      return new TenantArgs(volumeName, forceCreationWhenVolumeExists);
    }
  }

}
