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

import java.util.Objects;

/**
 * This class is used for storing Ranger Sync request args.
 */
public class OmRangerSyncArgs {

  /**
   * Ozone RangerServiceVersion to sync to.
   */
  private final long newServiceVersion;

  public OmRangerSyncArgs(long version) {
    this.newServiceVersion = version;
  }

  public long getNewSyncServiceVersion() {
    return newServiceVersion;
  }

  public static OmRangerSyncArgs.Builder newBuilder() {
    return new OmRangerSyncArgs.Builder();
  }

  /**
   * Builder for OmRangerSyncArgs.
   */
  public static class Builder {
    private long newServiceVersion;

    /**
     * Constructs a builder.
     */
    public Builder() {
    }

    public Builder setNewSyncServiceVersion(long version) {
      this.newServiceVersion = version;
      return this;
    }

    public OmRangerSyncArgs build() {
      Objects.requireNonNull(newServiceVersion, "newServiceVersion == null");
      return new OmRangerSyncArgs(newServiceVersion);
    }
  }
}
