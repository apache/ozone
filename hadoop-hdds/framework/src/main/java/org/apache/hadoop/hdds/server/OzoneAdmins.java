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

import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;

/**
 * This class contains ozone admin user information, username and group,
 * and is able to check whether the provided {@link UserGroupInformation}
 * has admin permissions.
 */
public class OzoneAdmins {

  /**
   * Ozone super user / admin username list.
   */
  private final Set<String> adminUsernames;
  /**
   * Ozone super user / admin group list.
   */
  private final Set<String> adminGroups;

  public OzoneAdmins(Collection<String> adminUsernames) {
    this(adminUsernames, null);
  }

  public OzoneAdmins(Collection<String> adminUsernames,
      Collection<String> adminGroups) {
    this.adminUsernames = adminUsernames != null ?
        Collections.unmodifiableSet(new LinkedHashSet<>(adminUsernames)) :
        Collections.emptySet();
    this.adminGroups = adminGroups != null ?
        Collections.unmodifiableSet(new LinkedHashSet<>(adminGroups)) :
        Collections.emptySet();
  }

  private boolean hasAdminGroup(Collection<String> userGroups) {
    return !Sets.intersection(adminGroups,
        new LinkedHashSet<>(userGroups)).isEmpty();
  }

  /**
   * Check whether the provided {@link UserGroupInformation user}
   * has admin permissions.
   *
   * @param user
   * @return
   */
  public boolean isAdmin(UserGroupInformation user) {
    return adminUsernames.contains(OZONE_ADMINISTRATORS_WILDCARD)
        || adminUsernames.contains(user.getShortUserName())
        || adminUsernames.contains(user.getUserName())
        || hasAdminGroup(user.getGroups());
  }

  public Collection<String> getAdminGroups() {
    return adminGroups;
  }

  public Set<String> getAdminUsernames() {
    return adminUsernames;
  }
}
