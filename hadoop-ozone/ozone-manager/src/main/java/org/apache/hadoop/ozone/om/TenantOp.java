/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.http.auth.BasicUserPrincipal;

import java.io.IOException;

/**
 * Tenant operation interface.
 */
public interface TenantOp {
  /**
   * Given a tenant name, create tenant roles and policies in the authorizer
   * (Ranger). Then return a Tenant object.
   * @param tenantID
   * @param userRoleName
   * @param adminRoleName
   */
  void createTenant(String tenantID, String userRoleName,
      String adminRoleName) throws IOException;

  /**
   * Given a Tenant object, remove all policies and roles from Ranger that are
   * added during tenant creation.
   * Note this differs from deactivateTenant() above.
   * @param tenant
   */
  void removeTenant(Tenant tenant) throws IOException;

  /**
   * Creates a new user that exists for S3 API access to Ozone.
   * @param principal
   * @param tenantId
   * @param accessId
   */
  void assignUserToTenant(BasicUserPrincipal principal,
      String tenantId, String accessId) throws IOException;

  /**
   * Revoke user accessId.
   * @param accessId
   */
  void revokeUserAccessId(String accessId, String tenantId) throws IOException;

  /**
   * Given a user, make him an admin of the corresponding Tenant.
   * This makes a request to Ranger.
   * @param accessId
   * @param delegated
   */
  void assignTenantAdmin(String accessId, boolean delegated)
      throws IOException;

  /**
   * Given an accessId, revoke that user's admin privilege in that tenant.
   */
  void revokeTenantAdmin(String accessId) throws IOException;
}
