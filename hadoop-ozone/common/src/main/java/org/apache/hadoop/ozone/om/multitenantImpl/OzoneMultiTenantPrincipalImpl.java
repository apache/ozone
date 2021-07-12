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
package org.apache.hadoop.ozone.om.multitenantImpl;

import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.USER_PRINCIPAL;

import java.security.Principal;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.http.auth.BasicUserPrincipal;

/**
 * Implements OzoneMultiTenantPrincipal.
 */
public class OzoneMultiTenantPrincipalImpl
    implements OzoneMultiTenantPrincipal {

  // TODO: This separator should come from Ozone Config.
  private static final String TENANT_ID_SEPARATOR =
      OzoneConsts.TENANT_NAME_USER_NAME_DELIMITER;
  private OzonePrincipalType  principalType = USER_PRINCIPAL;
  private Principal principalUserIDPart;
  private Principal principalTenantIDPart;

  public OzoneMultiTenantPrincipalImpl(Principal tenant, Principal user,
      OzonePrincipalType type) {
    principalTenantIDPart = tenant;
    principalUserIDPart = user;
    principalType = type;
  }

  @Override
  public String getFullMultiTenantPrincipalID() {
    return getTenantID() + TENANT_ID_SEPARATOR + getUserID();
  }

  @Override
  public Principal getPrincipal() {
    return new BasicUserPrincipal(getFullMultiTenantPrincipalID());
  }

  @Override
  public String getTenantID() {
    return principalTenantIDPart.getName();
  }

  @Override
  public String getUserID() {
    return principalUserIDPart.getName();
  }

  @Override
  public OzonePrincipalType getUserPrincipalType() {
    return principalType;
  }

  @Override
  public String toString() {
    return getFullMultiTenantPrincipalID();
  }
}
