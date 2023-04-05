/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.solr;

import org.apache.hadoop.ozone.recon.api.types.EntityReadAccessHeatMapResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * This is an abstract class acting as an interface for access to Solr Service.
 */
public abstract class AccessAuditsService {
  private List<SortField> sortFields = new ArrayList<SortField>();

  public AccessAuditsService() {
    sortFields.add(new SortField("eventCount", "event_count", true,
        SortField.SORTORDER.DESC));
    sortFields.add(new SortField("eventTime", "evtTime", false,
        SortField.SORTORDER.DESC));
    sortFields.add(new SortField(
        "policyId", "policy", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "requestUser", "reqUser", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "resourceType", "resType", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "accessType", "access", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "action", "action", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "aclEnforcer", "enforcer", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "zoneName", "zoneName", false, SortField.SORTORDER.ASC));
    sortFields.add(new SortField(
        "clientIP", "cliIP", false, SortField.SORTORDER.ASC));
  }

  public List<SortField> getSortFields() {
    return sortFields;
  }

  public abstract EntityReadAccessHeatMapResponse querySolrForOzoneAudit(
      String path, String entityType, String startDate);

}
