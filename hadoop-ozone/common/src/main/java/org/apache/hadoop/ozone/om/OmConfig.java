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

package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;

/**
 * Ozone Manager configuration.
 */
@ConfigGroup(prefix = "ozone.om")
public class OmConfig extends ReconfigurableConfig {

  /** This config needs to be enabled, when S3G created objects used via FileSystem API. */
  @Config(
      key = "enable.filesystem.paths",
      defaultValue = "false",
      description = "If true, key names will be interpreted as file system paths. " +
          "'/' will be treated as a special character and paths will be normalized " +
          "and must follow Unix filesystem path naming conventions. This flag will " +
          "be helpful when objects created by S3G need to be accessed using OFS/O3Fs. " +
          "If false, it will fallback to default behavior of Key/MPU create " +
          "requests where key paths are not normalized and any intermediate " +
          "directories will not be created or any file checks happens to check " +
          "filesystem semantics.",
      tags = { ConfigTag.OM, ConfigTag.OZONE }
  )
  private boolean fileSystemPathEnabled;

  @Config(
      key = "server.list.max.size",
      defaultValue = "1000",
      description = "Configuration property to configure the max server side response size for list calls on om.",
      reconfigurable = true,
      tags = { ConfigTag.OM, ConfigTag.OZONE }
  )
  private long maxListSize;

  @Config(
      key = "user.max.volume",
      defaultValue = "1024",
      description = "The maximum number of volumes a user can have on a cluster.Increasing or " +
          "decreasing this number has no real impact on ozone cluster. This is " +
          "defined only for operational purposes. Only an administrator can create a " +
          "volume, once a volume is created there are no restrictions on the number " +
          "of buckets or keys inside each bucket a user can create.",
      tags = { ConfigTag.OM, ConfigTag.MANAGEMENT }
  )
  private int maxUserVolumeCount;

  public boolean isFileSystemPathEnabled() {
    return fileSystemPathEnabled;
  }

  public void setFileSystemPathEnabled(boolean newValue) {
    fileSystemPathEnabled = newValue;
  }

  public long getMaxListSize() {
    return maxListSize;
  }

  public void setMaxListSize(long newValue) {
    maxListSize = newValue;
    validate();
  }

  public int getMaxUserVolumeCount() {
    return maxUserVolumeCount;
  }

  public void setMaxUserVolumeCount(int newValue) {
    maxUserVolumeCount = newValue;
    validate();
  }

  @PostConstruct
  public void validate() {
    if (maxListSize <= 0) {
      maxListSize = Defaults.SERVER_LIST_MAX_SIZE;
    }

    Preconditions.checkArgument(this.maxUserVolumeCount > 0,
        Keys.USER_MAX_VOLUME + " value should be greater than zero");
  }

  public OmConfig copy() {
    OmConfig copy = new OmConfig();
    copy.setFrom(this);
    return copy;
  }

  public void setFrom(OmConfig other) {
    fileSystemPathEnabled = other.fileSystemPathEnabled;
    maxListSize = other.maxListSize;
    maxUserVolumeCount = other.maxUserVolumeCount;
  }

  /**
   * String keys for tests and grep.
   */
  public static final class Keys {
    public static final String ENABLE_FILESYSTEM_PATHS = "ozone.om.enable.filesystem.paths";
    public static final String SERVER_LIST_MAX_SIZE = "ozone.om.server.list.max.size";
    public static final String USER_MAX_VOLUME = "ozone.om.user.max.volume";
  }

  /**
   * Default values for tests.
   */
  public static final class Defaults {
    public static final boolean ENABLE_FILESYSTEM_PATHS = false;
    public static final long SERVER_LIST_MAX_SIZE = 1000;
  }

}
