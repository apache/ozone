package org.apache.hadoop.ozone.om.multitenant;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class CachedTenantInfo {

  String tenantId;
  Set<Pair<String, String>> tenantUserAccessIds;

  public CachedTenantInfo(String tenantId) {
    this.tenantId = tenantId;
    tenantUserAccessIds = new HashSet<>();
  }

  public Set<Pair<String, String>> getTenantUsers() {
    return tenantUserAccessIds;
  }
}
