/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS_GROUPS;

/**
 * This class contains ozone admin user information, username and group,
 * and is able to check whether the provided {@link UserGroupInformation}
 * has admin permissions.
 */
public class OzoneAdmins {

  /**
   * Ozone super user / admin username list.
   */
  private volatile Set<String> adminUsernames;
  /**
   * Ozone super user / admin group list.
   */
  private final Set<String> adminGroups;

  public OzoneAdmins(Collection<String> adminUsernames) {
    this(adminUsernames, null);
  }

  public OzoneAdmins(Collection<String> adminUsernames,
      Collection<String> adminGroups) {
    setAdminUsernames(adminUsernames);
    this.adminGroups = adminGroups != null ?
        Collections.unmodifiableSet(new LinkedHashSet<>(adminGroups)) :
        Collections.emptySet();
  }

  /**
   * Returns an OzoneAdmins instance configured with admin users and groups
   * from the provided configuration. The starter user is added to the
   * admin user list if not already included.
   *
   * @param starterUser initial user to consider in admin list.
   * @param configuration the configuration settings to apply.
   * @return a configured OzoneAdmins instance.
   */
  public static OzoneAdmins getOzoneAdmins(String starterUser,
      OzoneConfiguration configuration) {
    Collection<String> adminUserNames =
        getOzoneAdminsFromConfig(configuration, starterUser);
    Collection<String> adminGroupNames =
        getOzoneAdminsGroupsFromConfig(configuration);
    return new OzoneAdmins(adminUserNames, adminGroupNames);
  }

  /**
   * Creates and returns a read-only admin object. This object includes the
   * read-only admin users and user groups obtained from the Ozone
   * configuration.
   *
   * @param configuration the configuration settings to apply.
   * @return a configured OzoneAdmins instance.
   */
  public static OzoneAdmins getReadonlyAdmins(
      OzoneConfiguration configuration) {
    Collection<String> omReadOnlyAdmins =
        getOzoneReadOnlyAdminsFromConfig(configuration);
    Collection<String> omReadOnlyAdminsGroups =
        getOzoneReadOnlyAdminsGroupsFromConfig(configuration);
    return new OzoneAdmins(omReadOnlyAdmins, omReadOnlyAdminsGroups);
  }

  /**
   * Check ozone admin privilege, throws exception if not admin.
   */
  public void checkAdminUserPrivilege(UserGroupInformation ugi)
      throws AccessControlException {
    if (ugi != null && !isAdmin(ugi)) {
      throw new AccessControlException("Access denied for user "
          + ugi.getUserName() + ". Superuser privilege is required.");
    }
  }

  private boolean hasAdminGroup(Collection<String> userGroups) {
    return !Sets.intersection(adminGroups,
        new LinkedHashSet<>(userGroups)).isEmpty();
  }

  /**
   * Check whether the provided {@link UserGroupInformation user}
   * has admin permissions.
   *
   * @param user the {@link UserGroupInformation}.
   * @return true if the user is an administrator, otherwise false.
   */
  public boolean isAdmin(UserGroupInformation user) {
    return user != null && (adminUsernames
        .contains(OZONE_ADMINISTRATORS_WILDCARD)
        || adminUsernames.contains(user.getShortUserName())
        || hasAdminGroup(user.getGroups()));
  }

  public Collection<String> getAdminGroups() {
    return adminGroups;
  }

  public Set<String> getAdminUsernames() {
    return adminUsernames;
  }

  public void setAdminUsernames(Collection<String> adminUsernames) {
    this.adminUsernames = adminUsernames != null ?
        Collections.unmodifiableSet(new LinkedHashSet<>(adminUsernames)) :
        Collections.emptySet();
  }

  /**
   * Return list of administrators from config.
   * The starterUser user will default to an admin.
   * @param conf the configuration settings to apply.
   * @param starterUser initial user to consider in admin list.
   */
  public static Collection<String> getOzoneAdminsFromConfig(
      OzoneConfiguration conf, String starterUser) {
    Collection<String> ozAdmins = conf.getTrimmedStringCollection(
        OZONE_ADMINISTRATORS);
    if (!ozAdmins.contains(starterUser)) {
      ozAdmins.add(starterUser);
    }
    return ozAdmins;
  }

  /**
   * Return list of administrators Groups from config.
   * The service startup user will default to an admin.
   * @param configuration the configuration settings to apply.
   */
  public static Collection<String> getOzoneAdminsGroupsFromConfig(
      OzoneConfiguration configuration) {
    return configuration.getTrimmedStringCollection(
        OZONE_ADMINISTRATORS_GROUPS);
  }

  /**
   * Return list of Ozone Read only admin Usernames from config.
   * @param conf the configuration settings to apply.
   */
  public static Collection<String> getOzoneReadOnlyAdminsFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(OZONE_READONLY_ADMINISTRATORS);
  }

  /**
   * Return list of Ozone Read only admin Groups from config.
   * @param conf the configuration settings to apply.
   */
  public static Collection<String> getOzoneReadOnlyAdminsGroupsFromConfig(
      OzoneConfiguration conf) {
    return conf.getTrimmedStringCollection(
        OZONE_READONLY_ADMINISTRATORS_GROUPS);
  }
}
