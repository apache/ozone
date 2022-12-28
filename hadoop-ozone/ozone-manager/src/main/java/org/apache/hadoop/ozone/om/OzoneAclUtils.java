/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

/**
 * Ozone Acl Wrapper class.
 */
public final class OzoneAclUtils {

  private static OMMultiTenantManager multiTenantManager;

  private OzoneAclUtils() {
  }

  public static void setOMMultiTenantManager(
      OMMultiTenantManager tenantManager) {
    multiTenantManager = tenantManager;
  }

  /**
   * Converts the given access ID to a kerberos principal.
   * If the access ID does not belong to a tenant, the access ID is returned
   * as is to be used as the principal.
   */
  public static String accessIdToUserPrincipal(String accessID) {
    if (multiTenantManager == null) {
      return accessID;
    }

    String principal = multiTenantManager.getUserNameGivenAccessId(accessID);
    if (principal == null) {
      principal = accessID;
    }

    return principal;
  }
}
