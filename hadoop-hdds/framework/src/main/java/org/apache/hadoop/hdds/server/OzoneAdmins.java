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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS;

import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

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
   * Return list of administrators from config value.
   * The starterUser user will default to an admin.
   * @param valueString the configuration value.
   * @param starterUser initial user to consider in admin list.
   */
  public static Collection<String> getOzoneAdminsFromConfigValue(
      String valueString, String starterUser) {
    Collection<String> ozAdmins = StringUtils.getTrimmedStringCollection(valueString);
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
   * Return list of Ozone Read only admin Usernames from config value.
   * @param valueString the configuration value.
   */
  public static Collection<String> getOzoneReadOnlyAdminsFromConfigValue(
      String valueString) {
    return StringUtils.getTrimmedStringCollection(valueString);
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

  /**
   * Get the list of S3 administrators from Ozone config.
   * <p/>
   * <strong>Notes</strong>:
   * <ul>
   *   <li>If <code>ozone.s3.administrators</code> value is empty string or unset,
   *       defaults to <code>ozone.administrators</code> value.</li>
   *   <li>If current user is not part of the administrators group,
   *       {@link UserGroupInformation#getCurrentUser()} will be added to the resulting list</li>
   * </ul>
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return A {@link Collection} of the S3 administrator users
   */
  public static Set<String> getS3AdminsFromConfig(OzoneConfiguration conf) throws IOException {
    Set<String> ozoneAdmins = new HashSet<>(conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS));

    if (ozoneAdmins.isEmpty()) {
      ozoneAdmins = new HashSet<>(conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS));
    }

    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    ozoneAdmins.add(omSPN);

    return ozoneAdmins;
  }

  /**
   * Get the list of the groups that are a part of S3 administrators from Ozone config.
   * <p/>
   * <strong>Note</strong>: If <code>ozone.s3.administrators.groups</code> value is empty or unset,
   * defaults to the <code>ozone.administrators.groups</code> value
   *
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return A {@link Collection} of the S3 administrator groups
   */
  public static Set<String> getS3AdminsGroupsFromConfig(OzoneConfiguration conf) {
    Set<String> s3AdminsGroup = new HashSet<>(conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS_GROUPS));

    if (s3AdminsGroup.isEmpty() && conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS).isEmpty()) {
      s3AdminsGroup = new HashSet<>(conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS_GROUPS));
    }

    return s3AdminsGroup;
  }

  /**
   * Get the users and groups that are a part of S3 administrators.
   * @param conf  Stores an instance of {@link OzoneConfiguration} being used
   * @return an instance of {@link OzoneAdmins} containing the S3 admin users and groups
   */
  public static OzoneAdmins getS3Admins(OzoneConfiguration conf) {
    Set<String> s3Admins;
    try {
      s3Admins = getS3AdminsFromConfig(conf);
    } catch (IOException ie) {
      s3Admins = Collections.emptySet();
    }
    Set<String> s3AdminGroups = getS3AdminsGroupsFromConfig(conf);

    return new OzoneAdmins(s3Admins, s3AdminGroups);
  }

  /**
   * Check if the provided user is an S3 administrator.
   * @param user An instance of {@link UserGroupInformation} with information about the user to verify
   * @param s3Admins An instance of {@link OzoneAdmins} containing information
   *                 of the S3 administrator users and groups in the system
   * @return {@code true} if the provided user is an S3 administrator else {@code false}
   */
  public static boolean isS3Admin(@Nullable UserGroupInformation user, OzoneAdmins s3Admins) {
    return null != user && s3Admins.isAdmin(user);
  }

  /**
   * Check if the provided user is an S3 administrator.
   * @param user An instance of {@link UserGroupInformation} with information about the user to verify
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return {@code true} if the provided user is an S3 administrator else {@code false}
   */
  public static boolean isS3Admin(@Nullable UserGroupInformation user, OzoneConfiguration conf) {
    OzoneAdmins s3Admins = getS3Admins(conf);
    return isS3Admin(user, s3Admins);
  }
}
