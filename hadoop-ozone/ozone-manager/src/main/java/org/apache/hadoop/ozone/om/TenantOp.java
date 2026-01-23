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

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.om.multitenant.Tenant;

/**
 * Interface for tenant operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface TenantOp {
  /**
   * Given a tenant name, create tenant roles and policies in the authorizer
   * (Ranger). Then return a Tenant object.
   *
   * @param tenantId tenant name
   * @param userRoleName user role name
   * @param adminRoleName admin role name
   */
  void createTenant(String tenantId, String userRoleName,
      String adminRoleName) throws IOException;

  /**
   * Given a Tenant object, remove all policies and roles from Ranger that are
   * added during tenant creation.
   * Note this differs from deactivateTenant() above.
   *
   * @param tenant tenant object
   */
  void deleteTenant(Tenant tenant) throws IOException;

  /**
   * Creates a new user that exists for S3 API access to Ozone.
   *
   * @param userPrincipal user principal
   * @param tenantId tenant name
   * @param accessId access ID
   */
  void assignUserToTenant(String userPrincipal,
      String tenantId, String accessId) throws IOException;

  /**
   * Revoke user accessId in a tenant.
   *
   * @param accessId access ID
   * @param tenantId tenant name
   */
  void revokeUserAccessId(String accessId, String tenantId) throws IOException;

  /**
   * Given an accessId, grant that user admin privilege in the tenant.
   *
   * @param accessId access ID
   * @param delegated true if intending to assign as the user as the tenant's
   *                  delegated admin (who can assign and revoke other tenant
   *                  admins in this tenant)
   */
  void assignTenantAdmin(String accessId, boolean delegated) throws IOException;

  /**
   * Given an accessId, revoke that user's admin privilege in that tenant.
   *
   * @param accessId access ID
   */
  void revokeTenantAdmin(String accessId) throws IOException;
}
