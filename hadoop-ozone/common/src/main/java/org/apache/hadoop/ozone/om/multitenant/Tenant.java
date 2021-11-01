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
package org.apache.hadoop.ozone.om.multitenant;

import java.util.List;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Tenant interface.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface Tenant {

  /**
   * A tenant is represnted by a globally unique TenantID.
   * @return Tenant-ID.
   */
  String getTenantId();

  /**
   * Return the AccountNameSpace for the Tenant.
   * @return AccountNameSpace.
   */
  AccountNameSpace getTenantAccountNameSpace();

  /**
   * @return BucketNameSpace for the Tenant.
   */
  BucketNameSpace getTenantBucketNameSpace();

  List<AccessPolicy> getTenantAccessPolicies();

  void addTenantAccessPolicy(AccessPolicy policy);

  void removeTenantAccessPolicy(AccessPolicy policy);

  void addTenantAccessRole(String groupID);

  void removeTenantAccessRole(String groupID);

  List<String> getTenantRoles();
}
