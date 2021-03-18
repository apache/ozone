package org.apache.hadoop.ozone.om.multitenantImpl;

import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.USER_PRINCIPAL;

import java.security.Principal;

import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.http.auth.BasicUserPrincipal;

public class OzoneMultiTenantPrincipalImpl implements OzoneMultiTenantPrincipal {
  // TODO: This separator should come from Ozone Config.
  final String tenantIDSeparator = "$";
  OzonePrincipalType  principalType = USER_PRINCIPAL;
  Principal principalUserIDPart;
  Principal principalTenantIDPart;

  public OzoneMultiTenantPrincipalImpl(Principal Tenant, Principal user,
                                OzonePrincipalType type) {
    principalTenantIDPart = Tenant;
    principalUserIDPart = user;
    principalType = type;
  }

  @Override
  public String getFullMultiTenantPrincipalID() {
    return getTenantID() + tenantIDSeparator + getUserID();
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
