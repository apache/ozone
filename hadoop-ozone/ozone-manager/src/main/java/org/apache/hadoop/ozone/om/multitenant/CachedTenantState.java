/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.multitenant;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A collection of things that we want to maintain about a tenant in memory.
 */
public class CachedTenantState {

  private String tenantId;
  private Set<Pair<String, String>> tenantUserAccessIds;

  public CachedTenantState(String tenantId) {
    this.tenantId = tenantId;
    tenantUserAccessIds = new HashSet<>();
  }

  public Set<Pair<String, String>> getTenantUsers() {
    return tenantUserAccessIds;
  }

  public String getTenantId() {
    return tenantId;
  }

  public boolean isTenantEmpty() {
    return tenantUserAccessIds.size() == 0;
  }
}
