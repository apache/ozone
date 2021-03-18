package org.apache.hadoop.ozone.om.multitenant;

import java.security.Principal;

public interface OzoneMultiTenantPrincipal {
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
  public String toString();
}
