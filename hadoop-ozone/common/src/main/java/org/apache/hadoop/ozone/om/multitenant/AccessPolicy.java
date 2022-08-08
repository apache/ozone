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

import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import com.google.gson.JsonObject;

/**
 * AccessPolicy interface for Ozone Multi-Tenancy.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface AccessPolicy {

  /**
   * Ozone could support different authorization engines e.g.
   * native-authorization, Ranger Authorization,
   * any-other-external-authorization. This interface is an in memory
   * version of a generic access policy. Any Ozone policy can be uniquely
   * identified by its policy-ID. Ozone can choose to persist this policy-ID
   * in its internal database. A remote/native authorizer can retrieve/update
   * an access policy associated with its Policy-ID ID.
   *
   */
  enum AccessPolicyType { NATIVE_ACL, RANGER_POLICY, AWS_POLICY, OTHER };

  /**
   * Allow or deny.
   */
  enum AccessGrantType { ALLOW, DENY };

  /**
   * Defines an access policy entry.
   */
  class AccessPolicyElem {
    private OzoneObj object;
    private Principal principal;
    private ACLType aclType;
    private AccessGrantType grantType;

    public AccessPolicyElem(OzoneObj obj, Principal id,
                     ACLType acl, AccessGrantType grant) {
      object = obj;
      principal = id;
      aclType = acl;
      grantType = grant;
    }

    public OzoneObj getObject() {
      return object;
    }

    public Principal getPrincipal() {
      return principal;
    }

    public ACLType getAclType() {
      return aclType;
    }

    public AccessGrantType getAccessGrantType() {
      return grantType;
    }
  }

  /**
   * @param id This would be policy-ID that an external/native authorizer
   *           could return.
   */
  void setPolicyName(String id);

  String getPolicyID();

  /**
   * @return unique policy-name for this policy.
   */
  String getPolicyName();

  /**
   *
   * @return Policy in a Json string format. Individual implementation can
   * choose different AccessPolicyType e.g. Ranger-Compatible-Json-Policy,
   * AWS-Compatible-Json-policy etc. It could be an Opaque data to the caller
   * and they can directly send it to an authorizer (e.g. Ranger).
   * All Authorizer policy engines are supposed to provide an implementation
   * of AccessPolicy interface.
   */
  String serializePolicyToJsonString() throws IOException;

  /**
   * Given a serialized accessPolicy in a Json format, deserializes and
   * constructs a valid access Policy.
   * @return
   * @throws IOException
   */
  String deserializePolicyFromJsonString(JsonObject jsonObject)
      throws IOException;

  /**
   * @return AccessPolicyType (Native or otherwise).
   */
  AccessPolicyType getAccessPolicyType();

  void addAccessPolicyElem(OzoneObj object,
                           Principal principal, ACLType acl,
                           AccessGrantType grant) throws IOException;

  void removeAccessPolicyElem(OzoneObj object,
                              Principal principal,
                              ACLType acl, AccessGrantType grant)
      throws IOException;

  List<AccessPolicyElem> getAccessPolicyElem();

  /**
   * Sets the last update time to mtime.
   * @param mtime Time in epoch milliseconds
   */
  void setPolicyLastUpdateTime(long mtime);

  /**
   * Returns the last update time of Ranger policies.
   */
  long getPolicyLastUpdateTime();

  /**
   * @return list of roles associated with this policy
   */
  HashSet<String> getRoleList();
}
