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

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessPolicyType.RANGER_POLICY;

/**
 * This is used for Ozone tenant access policy control.
 */
public class RangerAccessPolicy implements AccessPolicy {

  // For now RangerAccessPolicy supports only one object per policy
  private OzoneObj accessObject;
  private Map<String, List<AccessPolicyElem>> policyMap;
  private String policyID;
  private String policyJsonString;
  private String policyName;

  public RangerAccessPolicy(String name) {
    policyMap = new ConcurrentHashMap<>();
    policyName = name;
  }

  public void setPolicyID(String id) {
    policyID = id;
  }

  public String getPolicyID() {
    return policyID;
  }

  public String getPolicyName() {
    return policyName;
  }

  @Override
  public String serializePolicyToJsonString() throws Exception {
    updatePolicyJsonString();
    return policyJsonString;
  }

  @Override
  public String deserializePolicyFromJsonString(JsonObject jsonObject)
      throws Exception {
    setPolicyID(jsonObject.get("id").getAsString());
    // TODO : retrieve other policy fields as well.
    return null;
  }

  @Override
  public AccessPolicyType getAccessPolicyType() {
    return RANGER_POLICY;
  }

  @Override
  public void addAccessPolicyElem(OzoneObj object,
                                  Principal principal,
                                  ACLType acl, AccessGrantType grant)
      throws IOException {
    if (accessObject == null) {
      accessObject = object;
    } else if (!object.toString().equals(accessObject.toString())) {
      throw new IOException(
          "RangerAccessPolicy supports only one object per" + " policy");
    }
    AccessPolicyElem elem = new AccessPolicyElem(object, principal, acl, grant);
    if (!policyMap.containsKey(principal.getName())) {
      List<AccessPolicyElem> elemList = new ArrayList<>();
      elemList.add(elem);
      policyMap.put(principal.getName(), elemList);
      return;
    }
    List<AccessPolicyElem> elemList = policyMap.get(principal.getName());
    for (AccessPolicyElem e : elemList) {
      if (e.getAclType() == acl) {
        throw new IOException(
            "RangerAccessPolicy: Principal " + principal.getName()
                + " already exists with access " + acl);
      }
    }
    elemList.add(elem);
  }

  @Override
  public List<AccessPolicyElem> getAccessPolicyElem() {
    List<AccessPolicyElem> list = new ArrayList<>();
    for (Map.Entry<String, List<AccessPolicyElem>> entry : policyMap
        .entrySet()) {
      list.addAll(entry.getValue());
    }
    return list;
  }

  @Override
  public void removeAccessPolicyElem(OzoneObj object,
                                     Principal principal, ACLType acl,
                                     AccessGrantType grant)
      throws IOException {
    if (accessObject == null) {
      throw new IOException("removeAccessPolicyElem: Invalid Arguments.");
    } else if (!object.toString().equals(accessObject.toString())) {
      throw new IOException(
          "removeAccessPolicyElem:  Object not found." + object.toString());
    }
    if (!policyMap.containsKey(principal.getName())) {
      throw new IOException(
          "removeAccessPolicyElem:  Principal not found." + object.toString());
    }
    List<AccessPolicyElem> elemList = policyMap.get(principal.getName());
    for (AccessPolicyElem e : elemList) {
      if (e.getAclType() == acl) {
        elemList.remove(e);
      }
    }
    if (elemList.isEmpty()) {
      policyMap.remove(principal.toString());
    }
    throw new IOException(
        "removeAccessPolicyElem:  aclType not found." + object.toString());
  }

  private String createRangerResourceItems() throws IOException {
    StringBuilder resourceItems = new StringBuilder();
    resourceItems.append("\"resources\":{" +
        "\"volume\":{" +
        "\"values\":[\"");
    resourceItems.append(accessObject.getVolumeName());
    resourceItems.append("\"]," +
        "\"isRecursive\":false," +
        "\"isExcludes\":false" +
        "}");
    if ((accessObject.getResourceType() == OzoneObj.ResourceType.BUCKET) ||
        (accessObject.getResourceType() == OzoneObj.ResourceType.KEY)) {
      resourceItems.append(
          ",\"bucket\":{" +
          "\"values\":[\"");
      resourceItems.append(accessObject.getBucketName());
      resourceItems.append("\"]," +
          "\"isRecursive\":false," +
          "\"isExcludes\":false" +
          "}");
    }
    if (accessObject.getResourceType() == OzoneObj.ResourceType.KEY) {
      resourceItems.append(",\"key\":{" +
          "\"values\":[\"");
      resourceItems.append(accessObject.getKeyName());
      resourceItems.append("\"]," +
          "\"isRecursive\":true," +
          "\"isExcludes\":false" +
          "}");
    }
    resourceItems.append("},");
    return resourceItems.toString();
  }

  private String createRangerPolicyItems() throws IOException {
    StringBuilder policyItems = new StringBuilder();
    policyItems.append("\"policyItems\":[");
    int mapRemainingSize = policyMap.size();
    for (Map.Entry<String, List<AccessPolicyElem>> mapElem : policyMap
        .entrySet()) {
      mapRemainingSize--;
      List<AccessPolicyElem> list = mapElem.getValue();
      if (list.isEmpty()) {
        continue;
      }
      policyItems.append("{");
      if (list.get(0).getPrincipal() instanceof OzoneTenantGroupPrincipal) {
        policyItems.append("\"groups\":[\"" + mapElem.getKey() + "\"],");
      } else {
        policyItems.append("\"users\":[\"" + mapElem.getKey() + "\"],");
      }
      policyItems.append("\"accesses\":[");
      Iterator<AccessPolicyElem> iter = list.iterator();
      while (iter.hasNext()) {
        AccessPolicyElem elem = iter.next();
        policyItems.append("{");
        policyItems.append("\"type\":\"");
        policyItems.append(getRangerAclString(elem.getAclType()));
        policyItems.append("\",");
        if (elem.getAccessGrantType() == AccessGrantType.ALLOW) {
          policyItems.append("\"isAllowed\":true");
        } else {
          policyItems.append("\"isDenied\":true");
        }
        policyItems.append("}");
        if (iter.hasNext()) {
          policyItems.append(",");
        }
      }
      policyItems.append("]");
      policyItems.append("}");
      if (mapRemainingSize > 0) {
        policyItems.append(",");
      }
    }
    policyItems.append("],");
    return policyItems.toString();
  }

  private String getRangerAclString(ACLType aclType) throws IOException {
    switch (aclType) {
    case ALL:
      return "All";
    case LIST:
      return "List";
    case READ:
      return "Read";
    case WRITE:
      return "Write";
    case CREATE:
      return "Create";
    case DELETE:
      return "Delete";
    case READ_ACL:
      return "Read_ACL";
    case WRITE_ACL:
      return "Write_ACL";
    case NONE:
      return "";
    default:
      throw new IOException("Unknown ACLType");
    }
  }

  private void updatePolicyJsonString() throws Exception {
    policyJsonString =
        "{\"policyType\":\"0\"," + "\"name\":\"" + policyName + "\","
            + "\"isEnabled\":true," + "\"policyPriority\":0,"
            + "\"policyLabels\":[]," + "\"description\":\"\","
            + "\"isAuditEnabled\":true," + createRangerResourceItems()
            + "\"isDenyAllElse\":false," + createRangerPolicyItems()
            + "\"allowExceptions\":[]," + "\"denyPolicyItems\":[],"
            + "\"denyExceptions\":[]," + "\"service\":\"cm_ozone\"" + "}";
  }
}
