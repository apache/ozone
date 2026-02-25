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
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;

/**
 * Ozone Manager configuration.
 */
@ConfigGroup(prefix = "ozone.om")
public class OmConfig extends ReconfigurableConfig {

  /** This config needs to be enabled, when S3G created objects used via FileSystem API. */
  @Config(
      key = "ozone.om.enable.filesystem.paths",
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
      key = "ozone.om.keyname.character.check.enabled",
      defaultValue = "false",
      description = "If true, then enable to check if the key name " +
          "contains illegal characters when creating/renaming key. " +
          "For the definition of illegal characters, follow the " +
          "rules in Amazon S3's object key naming guide.",
      tags = { ConfigTag.OM, ConfigTag.OZONE }
  )
  private boolean keyNameCharacterCheckEnabled;

  @Config(
      key = "ozone.om.server.list.max.size",
      defaultValue = "1000",
      description = "Configuration property to configure the max server side response size for list calls on om.",
      reconfigurable = true,
      tags = { ConfigTag.OM, ConfigTag.OZONE }
  )
  private long maxListSize;

  @Config(
      key = "ozone.om.user.max.volume",
      defaultValue = "1024",
      description = "The maximum number of volumes a user can have on a cluster.Increasing or " +
          "decreasing this number has no real impact on ozone cluster. This is " +
          "defined only for operational purposes. Only an administrator can create a " +
          "volume, once a volume is created there are no restrictions on the number " +
          "of buckets or keys inside each bucket a user can create.",
      tags = { ConfigTag.OM, ConfigTag.MANAGEMENT }
  )
  private int maxUserVolumeCount;

  @Config(key = "ozone.om.upgrade.finalization.ratis.based.timeout",
      defaultValue = "30s",
      type = ConfigType.TIME,
      tags = {ConfigTag.OM, ConfigTag.UPGRADE},
      description = "Maximum time to wait for a slow follower to be finalized" +
          " through a Ratis snapshot. This is an advanced config, and needs " +
          "to be changed only under a special circumstance when the leader OM" +
          " has purged the finalize request from its logs, and a follower OM " +
          "was down during upgrade finalization. Default is 30s."
  )
  private long ratisBasedFinalizationTimeout = Duration.ofSeconds(30).getSeconds();

  // OM Default user/group permissions
  @Config(key = "ozone.om.user.rights",
      defaultValue = "ALL",
      type = ConfigType.STRING,
      tags = {ConfigTag.OM, ConfigTag.SECURITY},
      description = "Default user permissions set for an object in " +
          "OzoneManager."
  )
  private String userDefaultRights;
  private Set<ACLType> userDefaultRightSet;

  @Config(key = "ozone.om.group.rights",
      defaultValue = "READ, LIST",
      type = ConfigType.STRING,
      tags = {ConfigTag.OM, ConfigTag.SECURITY},
      description = "Default group permissions set for an object in " +
          "OzoneManager."
  )
  private String groupDefaultRights;
  private Set<ACLType> groupDefaultRightSet;

  @Config(key = "ozone.om.object.creation.ignore.client.acls",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = {ConfigTag.OM, ConfigTag.SECURITY},
      description = "Ignore ACLs sent by client to OzoneManager during volume/bucket/key creation."
  )
  private boolean ignoreClientACLs;

  @Config(key = "ozone.om.volume.listall.allowed",
      defaultValue = "true",
      reconfigurable = true,
      type = ConfigType.BOOLEAN,
      tags = {ConfigTag.OM, ConfigTag.MANAGEMENT},
      description =
          "Allows everyone to list all volumes when set to true. Defaults to true. " +
          "When set to false, non-admin users can only list the volumes they have " +
          "access to. Admins can always list all volumes. Note that this config " +
          "only applies to OzoneNativeAuthorizer. For other authorizers, admin " +
          "needs to set policies accordingly to allow all volume listing " +
          "e.g. for Ranger, a new policy with special volume \"/\" can be added to " +
          "allow group public LIST access."
  )
  private boolean listAllVolumesAllowed = Defaults.LIST_ALL_VOLUMES_ALLOWED;

  @Config(key = "ozone.om.allow.leader.skip.linearizable.read",
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      tags = {ConfigTag.OM, ConfigTag.PERFORMANCE, ConfigTag.HA},
      description = "Allow leader to handler requests directly, no need to check the leadership " +
          "for every request."
  )
  private boolean allowLeaderSkipLinearizableRead;

  @Config(key = "ozone.om.follower.read.local.lease.enabled",
      defaultValue = "false",
      reconfigurable = true,
      type = ConfigType.BOOLEAN,
      tags = {ConfigTag.OM, ConfigTag.PERFORMANCE, ConfigTag.HA, ConfigTag.RATIS},
      description = "If we enabled the local lease for Follower Read. " +
          "If enabled, follower OM will decide if return local data directly\n" +
          "based on lag log and time."
  )
  private boolean followerReadLocalLeaseEnabled;

  @Config(key = "ozone.om.follower.read.local.lease.log.limit",
      defaultValue = "10000",
      reconfigurable = true,
      type = ConfigType.LONG,
      tags = {ConfigTag.OM, ConfigTag.PERFORMANCE, ConfigTag.HA, ConfigTag.RATIS},
      description = "If the log lag between leader OM and follower OM is larger " +
          "than this number, the follower OM is not up-to-date."
  )
  private long followerReadLocalLeaseLagLimit;

  @Config(key = "ozone.om.follower.read.local.lease.time.ms",
      defaultValue = "5000",
      reconfigurable = true,
      type = ConfigType.LONG,
      tags = {ConfigTag.OM, ConfigTag.PERFORMANCE, ConfigTag.HA, ConfigTag.RATIS},
      description = " If the lag time Ms between leader OM and follower OM is larger " +
          "than this number, the follower OM is not up-to-date. " +
          "By default, it's set to Ratis RPC timeout value."
  )
  private long followerReadLocalLeaseTimeMs;

  public long getRatisBasedFinalizationTimeout() {
    return ratisBasedFinalizationTimeout;
  }

  public boolean isFileSystemPathEnabled() {
    return fileSystemPathEnabled;
  }

  public void setFileSystemPathEnabled(boolean newValue) {
    fileSystemPathEnabled = newValue;
  }

  public boolean isListAllVolumesAllowed() {
    return listAllVolumesAllowed;
  }

  public void setListAllVolumesAllowed(boolean newValue) {
    listAllVolumesAllowed = newValue;
  }

  public boolean isKeyNameCharacterCheckEnabled() {
    return keyNameCharacterCheckEnabled;
  }

  public void setKeyNameCharacterCheckEnabled(boolean newValue) {
    this.keyNameCharacterCheckEnabled = newValue;
  }

  public long getMaxListSize() {
    return maxListSize;
  }

  public boolean isAllowLeaderSkipLinearizableRead() {
    return allowLeaderSkipLinearizableRead;
  }

  public void setAllowLeaderSkipLinearizableRead(boolean newValue) {
    allowLeaderSkipLinearizableRead = newValue;
  }

  public boolean isFollowerReadLocalLeaseEnabled() {
    return followerReadLocalLeaseEnabled;
  }

  public void setFollowerReadLocalLeaseEnabled(boolean newValue) {
    this.followerReadLocalLeaseEnabled = newValue;
  }

  public long getFollowerReadLocalLeaseLagLimit() {
    return followerReadLocalLeaseLagLimit;
  }

  public void setFollowerReadLocalLeaseLagLimit(long newValue) {
    this.followerReadLocalLeaseLagLimit = newValue;
  }

  public long getFollowerReadLocalLeaseTimeMs() {
    return followerReadLocalLeaseTimeMs;
  }

  public void setFollowerReadLocalLeaseTimeMs(long newValue) {
    this.followerReadLocalLeaseTimeMs = newValue;
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

  public Set<ACLType> getUserDefaultRights() {
    if (userDefaultRightSet == null) {
      userDefaultRightSet = getUserDefaultRightSet();
    }
    return userDefaultRightSet;
  }

  private Set<ACLType> getUserDefaultRightSet() {
    return userDefaultRights == null
        ? Collections.singleton(ACLType.ALL)
        : ACLType.parseList(userDefaultRights);
  }

  public Set<ACLType> getGroupDefaultRights() {
    if (groupDefaultRightSet == null) {
      groupDefaultRightSet = getGroupDefaultRightSet();
    }
    return groupDefaultRightSet;
  }

  private Set<ACLType> getGroupDefaultRightSet() {
    return groupDefaultRights == null
        ? Collections.unmodifiableSet(EnumSet.of(ACLType.READ, ACLType.LIST))
        : ACLType.parseList(groupDefaultRights);
  }

  public boolean ignoreClientACLs() {
    return ignoreClientACLs;
  }

  public void setIgnoreClientACLs(boolean ignore) {
    ignoreClientACLs = ignore;
  }

  @PostConstruct
  public void validate() {
    if (maxListSize <= 0) {
      maxListSize = Defaults.SERVER_LIST_MAX_SIZE;
    }

    Preconditions.checkArgument(this.maxUserVolumeCount > 0,
        Keys.USER_MAX_VOLUME + " value should be greater than zero");

    userDefaultRightSet = getUserDefaultRightSet();
    groupDefaultRightSet = getGroupDefaultRightSet();
  }

  public OmConfig copy() {
    OmConfig copy = new OmConfig();
    copy.setFrom(this);
    return copy;
  }

  public void setFrom(OmConfig other) {
    fileSystemPathEnabled = other.fileSystemPathEnabled;
    keyNameCharacterCheckEnabled = other.keyNameCharacterCheckEnabled;
    listAllVolumesAllowed = other.listAllVolumesAllowed;
    maxListSize = other.maxListSize;
    maxUserVolumeCount = other.maxUserVolumeCount;
    userDefaultRights = other.userDefaultRights;
    groupDefaultRights = other.groupDefaultRights;
    allowLeaderSkipLinearizableRead = other.allowLeaderSkipLinearizableRead;

    validate();
  }

  /**
   * String keys for tests and grep.
   */
  public static final class Keys {
    public static final String ENABLE_FILESYSTEM_PATHS = "ozone.om.enable.filesystem.paths";
    public static final String LIST_ALL_VOLUMES_ALLOWED = "ozone.om.volume.listall.allowed";
    public static final String SERVER_LIST_MAX_SIZE = "ozone.om.server.list.max.size";
    public static final String USER_MAX_VOLUME = "ozone.om.user.max.volume";
  }

  /**
   * Default values for tests.
   */
  public static final class Defaults {
    public static final boolean ENABLE_FILESYSTEM_PATHS = false;
    public static final boolean LIST_ALL_VOLUMES_ALLOWED = true;
    public static final long SERVER_LIST_MAX_SIZE = 1000;
  }

}
