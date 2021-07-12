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
package org.apache.hadoop.ozone.om.multitenant;

import java.security.Principal;

/**
 * OzoneMultiTenantPrincipal interface.
 */
public interface OzoneMultiTenantPrincipal {

  /**
   * Two types of Ozone principal.
   */
  enum OzonePrincipalType{USER_PRINCIPAL, GROUP_PRINCIPAL};

  /**
   * @return Principal(access-id) representing the multiTenantUser including
   * any Tenant AccountNameSpace qualification.
   */
  Principal getPrincipal();

  /**
   * @return full String ID of the MultiTenantPrincipal
   */
  String getFullMultiTenantPrincipalID();

  /**
   * @return plain TenantID part
   */
  String getTenantID();

  /**
   * @return plain userID part
   */
  String getUserID();

  /**
   *
   * @return Whether this principal represents a User or a group.
   */
  OzonePrincipalType getUserPrincipalType();

  @Override
  String toString();
}
