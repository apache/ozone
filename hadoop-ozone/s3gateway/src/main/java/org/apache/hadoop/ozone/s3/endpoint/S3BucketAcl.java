/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Bucket ACL.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "AccessControlPolicy",
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class S3BucketAcl {

  @XmlElement(name = "Owner")
  private S3Owner owner;

  @XmlElement(name = "AccessControlList")
  private AccessControlList aclList;

  public S3Owner getOwner() {
    return owner;
  }

  public void setOwner(S3Owner owner) {
    this.owner = owner;
  }

  public AccessControlList getAclList() {
    return aclList;
  }

  public void setAclList(AccessControlList aclList) {
    this.aclList = aclList;
  }

  @Override
  public String toString() {
    return "GetBucketAclResponse{" +
        "owner=" + owner +
        ", aclList=" + aclList +
        '}';
  }

  /**
   * TODO: javadoc.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "AccessControlList")
  public static class AccessControlList {

    @XmlElement(name = "Grant")
    private List<Grant> grantList = new ArrayList<>();

    public void addGrant(Grant grant) {
      grantList.add(grant);
    }

    public List<Grant> getGrantList() {
      return grantList;
    }

    public AccessControlList(List<Grant> grants) {
      this.grantList = grants;
    }

    public AccessControlList() {

    }

    @Override
    public String toString() {
      return "AccessControlList{" +
          "grantList=" + grantList +
          '}';
    }
  }

  /**
   * TODO: javadoc.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Grant")
  public static class Grant {

    @XmlElement(name = "Grantee")
    private Grantee grantee;

    @XmlElement(name = "Permission")
    private String permission;

    public String getPermission() {
      return permission;
    }

    public void setPermission(String permission) {
      this.permission = permission;
    }

    public Grantee getGrantee() {
      return grantee;
    }

    public void setGrantee(Grantee grantee) {
      this.grantee = grantee;
    }

    @Override
    public String toString() {
      return "Grant{" +
          "grantee=" + grantee +
          ", permission='" + permission + '\'' +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Grant grant = (Grant) o;
      return Objects.equals(grantee, grant.grantee) &&
          Objects.equals(permission, grant.permission);
    }

    @Override
    public int hashCode() {
      return Objects.hash(grantee, permission);
    }
  }

  /**
   * A grantee can be an AWS account or one of the predefined Amazon S3 groups.
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Grantee")
  public static class Grantee {

    @XmlElement(name = "DisplayName")
    private String displayName;

    @XmlElement(name = "ID")
    private String id;

    @XmlAttribute(name="xsi:type")
    private String xsiType = "CanonicalUser";

    @XmlAttribute(name="xmlns:xsi")
    private String xsiNs = "http://www.w3.org/2001/XMLSchema-instance";

    public String getXsiNs() {
      return xsiNs;
    }

    public String getXsiType() {
      return xsiType;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getDisplayName() {
      return displayName;
    }

    public void setDisplayName(String name) {
      this.displayName = name;
    }

    public void setXsiType(String type) {
      this.xsiType = type;
    }

    public void setXsiNs(String ns) {
      this.xsiNs = ns;
    }

    @Override
    public String toString() {
      return "Grantee{" +
          "displayName='" + displayName + '\'' +
          ", id='" + id + '\'' +
          ", xsiType='" + xsiType + '\'' +
          ", xsiNs='" + xsiNs + '\'' +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Grantee grantee = (Grantee) o;
      return Objects.equals(displayName, grantee.displayName) &&
          Objects.equals(id, grantee.id) &&
          Objects.equals(xsiType, grantee.xsiType) &&
          Objects.equals(xsiNs, grantee.xsiNs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(displayName, id, xsiType, xsiNs);
    }
  }
}
