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

package org.apache.hadoop.hdds.server;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLACKLIST_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLACKLIST_USERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READ_BLACKLIST_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READ_BLACKLIST_USERS;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * This class contains the blacklisted user information, username and group
 * and is able to check whether the provided {@link UserGroupInformation}
 * is blacklisted.
 *
 * The implementation is similar to {@link OzoneAdmins}.
 */
public class OzoneBlacklist {

  private volatile Set<String> blacklistUsernames;

  private volatile Set<String> blacklistGroups;

  public OzoneBlacklist(Collection<String> blacklistUsernames) {
    this(blacklistUsernames, null);
  }

  public OzoneBlacklist(Collection<String> blacklistUsernames,
      Collection<String> blacklistGroups) {
    setBlacklistUsernames(blacklistUsernames);
    this.blacklistGroups = blacklistGroups != null
        ? Collections.unmodifiableSet(new LinkedHashSet<>(blacklistGroups))
        : Collections.emptySet();
  }

  /**
   * Returns an OzoneBlacklist instance configured with blacklisted users
   * and groups from the provided configuration.
   *
   * @param configuration the configuration settings to apply.
   * @return a configured OzoneBlacklist instance.
   */
  public static OzoneBlacklist getOzoneBlacklist(
      OzoneConfiguration configuration) {
    Collection<String> blacklistUsernames =
        getOzoneBlacklistUsersFromConfig(configuration);
    Collection<String> blacklistGroupNames =
        getOzoneBlacklistGroupsFromConfig(configuration);
    return new OzoneBlacklist(blacklistUsernames, blacklistGroupNames);
  }

  /**
   * Creates and returns a read-only blacklist object. This object includes the
   * read blacklisted users and user groups obtained from the Ozone
   * configuration.
   *
   * @param configuration the configuration settings to apply.
   * @return a configured OzoneBlacklist instance.
   */
  public static OzoneBlacklist getReadonlyBlacklist(
      OzoneConfiguration configuration) {
    Collection<String> omReadBlacklistUser =
        getOzoneReadBlacklistUsersFromConfig(configuration);
    Collection<String> omReadBlacklistGroups =
        getOzoneReadBlacklistGroupsFromConfig(configuration);
    return new OzoneBlacklist(omReadBlacklistUser, omReadBlacklistGroups);
  }

  /**
   * Check ozone blacklist, throws exception if user is blacklisted.
   */
  public void checkBlacklist(UserGroupInformation ugi)
      throws AccessControlException {
    if (ugi != null && isBlacklisted(ugi)) {
      throw new AccessControlException("Access denied for user "
          + ugi.getUserName() + ". User is blacklisted.");
    }
  }

  private boolean hasBlacklistGroup(Collection<String> userGroups) {
    return !Sets.intersection(blacklistGroups,
        new LinkedHashSet<>(userGroups)).isEmpty();
  }

  /**
   * Check whether the provided {@link UserGroupInformation user}
   * is blacklisted.
   *
   * @param user the {@link UserGroupInformation}.
   * @return true if the user is blacklisted, otherwise false.
   */
  public boolean isBlacklisted(UserGroupInformation user) {
    return user != null
        && (blacklistUsernames.contains(user.getShortUserName())
        || hasBlacklistGroup(user.getGroups()));
  }

  public Collection<String> getBlacklistGroups() {
    return blacklistGroups;
  }

  public Set<String> getBlacklistUsernames() {
    return blacklistUsernames;
  }

  public void setBlacklistUsernames(
      Collection<String> blacklistUsernames) {
    this.blacklistUsernames = blacklistUsernames != null
        ? Collections.unmodifiableSet(
            new LinkedHashSet<>(blacklistUsernames))
        : Collections.emptySet();
  }

  /**
   * Return list of blacklisted users from config.
   *
   * @param conf the configuration settings to apply.
   */
  public static Collection<String> getOzoneBlacklistUsersFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_BLACKLIST_USERS);
  }

  /**
   * Return list of blacklisted users from config value.
   * @param valueString the configuration value.
   */
  public static Collection<String> getOzoneBlacklistUsersFromConfigValue(
      String valueString) {
    return StringUtils.getTrimmedStringCollection(valueString);
  }

  /**
   * Return list of blacklist Groups from config.
   *
   * @param configuration the configuration settings to apply.
   */
  public static Collection<String> getOzoneBlacklistGroupsFromConfig(
      OzoneConfiguration configuration) {
    return configuration.getTrimmedStringCollection(OZONE_BLACKLIST_GROUPS);
  }

  /**
   * Return list of blacklisted groups from config value.
   * @param valueString the configuration value.
   */
  public static Collection<String> getOzoneBlacklistGroupsFromConfigValue(
      String valueString) {
    return StringUtils.getTrimmedStringCollection(valueString);
  }

  /**
   * Return list of Ozone Read only blacklisted users from config.
   *
   * @param conf the configuration settings to apply.
   */
  public static Collection<String> getOzoneReadBlacklistUsersFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_READ_BLACKLIST_USERS);
  }

  /**
   * Return list of Ozone Read only blacklisted users from config value.
   * @param valueString the configuration value.
   */
  public static Collection<String> getOzoneReadBlacklistUsersFromConfigValue(
      String valueString) {
    return StringUtils.getTrimmedStringCollection(valueString);
  }

  /**
   * Return list of Ozone Read only blacklisted groups from config.
   *
   * @param conf the configuration settings to apply.
   */
  public static Collection<String> getOzoneReadBlacklistGroupsFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_READ_BLACKLIST_GROUPS);
  }

  /**
   * Return list of Ozone Read only blacklisted groups from config value.
   * @param valueString the configuration value.
   */
  public static Collection<String> getOzoneReadBlacklistGroupsFromConfigValue(
      String valueString) {
    return StringUtils.getTrimmedStringCollection(valueString);
  }

  public void setBlacklistGroups(Collection<String> blacklistGroups) {
    this.blacklistGroups = blacklistGroups != null
        ? Collections.unmodifiableSet(new LinkedHashSet<>(blacklistGroups))
        : Collections.emptySet();
  }
}
