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

package org.apache.hadoop.ozone.container.diskbalancer;

import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Defines versions for the DiskBalancerService.
 */
public enum DiskBalancerVersion {
  ONE(1, "First Version") {
  };

  private final int version;
  private final String description;

  public static final DiskBalancerVersion
      DEFAULT_VERSION = DiskBalancerVersion.ONE;

  private static final List<DiskBalancerVersion> DISK_BALANCER_VERSIONS =
      ImmutableList.copyOf(values());

  DiskBalancerVersion(int version, String description) {
    this.version = version;
    this.description = description;
  }

  public static DiskBalancerVersion getDiskBalancerVersion(int version) {
    for (DiskBalancerVersion diskBalancerVersion :
        DISK_BALANCER_VERSIONS) {
      if (diskBalancerVersion.getVersion() == version) {
        return diskBalancerVersion;
      }
    }
    return null;
  }

  public static DiskBalancerVersion getDiskBalancerVersion(String versionStr) {
    for (DiskBalancerVersion diskBalancerVersion :
        DISK_BALANCER_VERSIONS) {
      if (diskBalancerVersion.toString().equalsIgnoreCase(versionStr)) {
        return diskBalancerVersion;
      }
    }
    return null;
  }

  /**
   * @return version number.
   */
  public int getVersion() {
    return version;
  }

  /**
   * @return description.
   */
  public String getDescription() {
    return description;
  }
}
