/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.s3.endpoint.S3BucketAcl.Grant;
import org.apache.hadoop.ozone.s3.endpoint.S3BucketAcl.Grantee;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;

public final class S3Acl {
  private static final Logger LOG = LoggerFactory.getLogger(S3Acl.class);

  // ACL put related headers
  public static final String GRANT_READ = "x-amz-grant-read";
  public static final String GRANT_WRITE = "x-amz-grant-write";
  public static final String GRANT_READ_CAP = "x-amz-grant-read-acp";
  public static final String GRANT_WRITE_CAP = "x-amz-grant-write-acp";
  public static final String GRANT_FULL_CONTROL = "x-amz-grant-full-control";

  // Not supported headers at current stage, may support it in future
  public static final String CANNED_ACL_HEADER = "x-amz-acl";

  /**
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html.
   */
  enum ACLType {
    // Allows grantee to list the objects in the bucket
    READ("READ"),
    // Allows grantee to create, overwrite, and delete any object in the bucket
    WRITE("WRITE"),
    // Allows grantee to write the bucket ACL
    READ_ACP("READ_ACP"),
    // Allows grantee to write the ACL for the applicable bucket
    WRITE_ACP("WRITE_ACP"),
    // Allows grantee above all permissions on the bucket
    FULL_CONTROL("FULL_CONTROL");

    public String getValue() {
      return value;
    }
    /**
     * String value for this Enum.
     */
    private final String value;

    /**
     * @param val String type for this enum.
     */
    ACLType(String val) {
      value = val;
    }


    public static ACLType getType(String typeStr) {
      for(ACLType type: ACLType.values()) {
        if (type.getValue().equals(typeStr)) {
          return type;
        }
      }
      return null;
    }
  }

  enum ACLIdentityType {
    USER("CanonicalUser", true, "id"),
    GROUP("Group", false, "url"),
    USER_BY_EMAIL("AmazonCustomerByEmail", false, "emailAddress");

    public String getGranteeType() {
      return granteeType;
    }

    public String getHeaderType() {
      return granteeInHeader;
    }

    /**
     *  Grantee type in body XML.
     */
    private final String granteeType;

    /**
     * Is this type supported or not.
     */
    private final boolean supported;

    /**
     * Grantee type in header.
     */
    private final String granteeInHeader;

    /**
     * Init OzoneACLtypes enum.
     *
     * @param val String type for this enum.
     */
    ACLIdentityType(String val, boolean support, String headerType) {
      granteeType = val;
      supported = support;
      granteeInHeader = headerType;
    }

    boolean isSupported() {
      return supported;
    }

    public static ACLIdentityType getTypeFromGranteeType(String typeStr) {
      for(ACLIdentityType type: ACLIdentityType.values()) {
        if (type.getGranteeType().equals(typeStr)) {
          return type;
        }
      }
      return null;
    }

    public static ACLIdentityType getTypeFromHeaderType(String typeStr) {
      for(ACLIdentityType type: ACLIdentityType.values()) {
        if (type.getHeaderType().equals(typeStr)) {
          return type;
        }
      }
      return null;
    }
  }

  private S3Acl() {
  }

  public static boolean isGranteeTypeSupported(String typeStr) {
    ACLIdentityType type =  ACLIdentityType.getTypeFromGranteeType(typeStr);
    return type == null ? false : type.isSupported();
  }

  public static boolean isHeaderTypeSupported(String typeStr) {
    ACLIdentityType type =  ACLIdentityType.getTypeFromHeaderType(typeStr);
    return type == null ? false : type.isSupported();
  }

  public static List<Grant> ozoneNativeAclToS3Acl(OzoneAcl ozoneAcl) {
    // Since currently only "CanonicalUser" is supported, which maps to Ozone
    // "USER"
    List<Grant> grantList = new ArrayList<>();
    if (ozoneAcl.getType() != IAccessAuthorizer.ACLIdentityType.USER) {
      return grantList;
    }

    Grantee grantee = new Grantee();
    grantee.setDisplayName(ozoneAcl.getName());
    grantee.setId(ozoneAcl.getName());

    List<IAccessAuthorizer.ACLType> acls = ozoneAcl.getAclList();
    if (acls.contains(IAccessAuthorizer.ACLType.ALL)) {
      Grant grant = new Grant();
      grant.setGrantee(grantee);
      grant.setPermission(ACLType.FULL_CONTROL.toString());
      grantList.add(grant);
    } else if (acls.contains(IAccessAuthorizer.ACLType.WRITE_ACL)) {
      Grant grant = new Grant();
      grant.setGrantee(grantee);
      grant.setPermission(ACLType.WRITE_ACP.toString());
      grantList.add(grant);
    } else if (acls.contains(IAccessAuthorizer.ACLType.READ_ACL)) {
      Grant grant = new Grant();
      grant.setGrantee(grantee);
      grant.setPermission(ACLType.READ_ACP.toString());
      grantList.add(grant);
    } else if (acls.contains(IAccessAuthorizer.ACLType.WRITE) &&
        acls.contains(IAccessAuthorizer.ACLType.DELETE) &&
        acls.contains(IAccessAuthorizer.ACLType.CREATE)) {
      Grant grant = new Grant();
      grant.setGrantee(grantee);
      grant.setPermission(ACLType.WRITE.toString());
      grantList.add(grant);
    } else if (acls.contains(IAccessAuthorizer.ACLType.READ) &&
        acls.contains(IAccessAuthorizer.ACLType.LIST)) {
      Grant grant = new Grant();
      grant.setGrantee(grantee);
      grant.setPermission(ACLType.READ.toString());
      grantList.add(grant);
    } else {
      LOG.error("Cannot find a good mapping for Ozone ACL {} to S3",
          ozoneAcl.toString());
    }
    return grantList;
  }

  public static List<OzoneAcl> s3AclToOzoneNativeAclOnBucket(
      S3BucketAcl bucketAcl) throws OS3Exception {
    List<OzoneAcl> ozoneAclList = new ArrayList<>();
    List<Grant> grantList = bucketAcl.getAclList().getGrantList();
    for (Grant grant : grantList) {
      //  Only "CanonicalUser" is supported, which maps to Ozone "USER"
      ACLIdentityType identityType = ACLIdentityType.getTypeFromGranteeType(
          grant.getGrantee().getXsiType());
      if (identityType != null && identityType.isSupported()) {
        String permission = grant.getPermission();
        BitSet acls = getOzoneAclOnBucketFromS3Permission(permission);
        OzoneAcl defaultOzoneAcl = new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.USER,
            grant.getGrantee().getId(), acls,
            OzoneAcl.AclScope.DEFAULT);
        OzoneAcl accessOzoneAcl = new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.USER,
            grant.getGrantee().getId(), acls,
            OzoneAcl.AclScope.ACCESS);
        ozoneAclList.add(defaultOzoneAcl);
        ozoneAclList.add(accessOzoneAcl);
      } else {
        LOG.error("Grantee type {} is not supported",
            grant.getGrantee().getXsiType());
        throw S3ErrorTable.newError(NOT_IMPLEMENTED,
            grant.getGrantee().getXsiType());
      }
    }
    return ozoneAclList;
  }

  public static BitSet getOzoneAclOnBucketFromS3Permission(String permission)
      throws OS3Exception {
    ACLType permissionType = ACLType.getType(permission);
    if (permissionType == null) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, permission);
    }
    BitSet acls = new BitSet(IAccessAuthorizer.ACLType.getNoOfAcls());
    switch (permissionType) {
    case FULL_CONTROL:
      acls.set(IAccessAuthorizer.ACLType.ALL.ordinal());
      break;
    case WRITE_ACP:
      acls.set(IAccessAuthorizer.ACLType.WRITE_ACL.ordinal());
      break;
    case READ_ACP:
      acls.set(IAccessAuthorizer.ACLType.READ_ACL.ordinal());
      break;
    case WRITE:
      acls.set(IAccessAuthorizer.ACLType.WRITE.ordinal());
      acls.set(IAccessAuthorizer.ACLType.DELETE.ordinal());
      acls.set(IAccessAuthorizer.ACLType.CREATE.ordinal());
      break;
    case READ:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      acls.set(IAccessAuthorizer.ACLType.LIST.ordinal());
      break;
    default:
      LOG.error("Failed to recognize S3 permission {}", permission);
      throw S3ErrorTable.newError(INVALID_ARGUMENT, permission);
    }
    return acls;
  }

  public static List<OzoneAcl> s3AclToOzoneNativeAclOnVolume(
      S3BucketAcl bucketAcl) throws OS3Exception {
    List<OzoneAcl> ozoneAclList = new ArrayList<>();
    List<Grant> grantList = bucketAcl.getAclList().getGrantList();
    for (Grant grant : grantList) {
      //  Only "CanonicalUser" is supported, which maps to Ozone "USER"
      ACLIdentityType identityType = ACLIdentityType.getTypeFromGranteeType(
          grant.getGrantee().getXsiType());
      if (identityType != null && identityType.isSupported()) {
        String permission = grant.getPermission();
        BitSet acls = getOzoneAclOnVolumeFromS3Permission(permission);
        OzoneAcl accessOzoneAcl = new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.USER,
            grant.getGrantee().getId(), acls,
            OzoneAcl.AclScope.ACCESS);
        ozoneAclList.add(accessOzoneAcl);
      } else {
        LOG.error("Grantee type {} is not supported",
            grant.getGrantee().getXsiType());
        throw S3ErrorTable.newError(NOT_IMPLEMENTED,
            grant.getGrantee().getXsiType());
      }
    }
    return ozoneAclList;
  }

  // User privilege on volume follows the "lest privilege" principle.
  public static BitSet getOzoneAclOnVolumeFromS3Permission(String permission)
      throws OS3Exception {
    BitSet acls = new BitSet(IAccessAuthorizer.ACLType.getNoOfAcls());
    ACLType permissionType = ACLType.getType(permission);
    if (permissionType == null) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, permission);
    }
    switch (permissionType) {
    case FULL_CONTROL:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      acls.set(IAccessAuthorizer.ACLType.WRITE.ordinal());
      acls.set(IAccessAuthorizer.ACLType.READ_ACL.ordinal());
      acls.set(IAccessAuthorizer.ACLType.WRITE_ACL.ordinal());
      break;
    case WRITE_ACP:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      acls.set(IAccessAuthorizer.ACLType.READ_ACL.ordinal());
      acls.set(IAccessAuthorizer.ACLType.WRITE_ACL.ordinal());
      break;
    case READ_ACP:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      acls.set(IAccessAuthorizer.ACLType.READ_ACL.ordinal());
      break;
    case WRITE:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      acls.set(IAccessAuthorizer.ACLType.WRITE.ordinal());
      break;
    case READ:
      acls.set(IAccessAuthorizer.ACLType.READ.ordinal());
      break;
    default:
      LOG.error("Failed to recognize S3 permission {}", permission);
      throw S3ErrorTable.newError(INVALID_ARGUMENT, permission);
    }
    return acls;
  }
}
