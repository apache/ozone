/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for bucket ACL operations (?acl query parameter).
 * Implements PUT operations for bucket Access Control Lists.
 *
 * This handler extends EndpointBase to inherit all required functionality
 * (configuration, headers, request context, audit logging, metrics, etc.).
 */
public class AclHandler extends EndpointBase implements BucketOperationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AclHandler.class);

  /**
   * Determine if this handler should handle the current request.
   * @return true if the request has the "acl" query parameter
   */
  private boolean shouldHandle() {
    return queryParams().get(QueryParams.ACL) != null;
  }

  /**
   * Implement acl put.
   * <p>
   * see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
   */
  @Override
  public Response handlePutRequest(String bucketName, InputStream body)
      throws IOException, OS3Exception {

    if (!shouldHandle()) {
      return null;  // Not responsible for this request
    }

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.PUT_ACL;

    String grantReads = getHeaders().getHeaderString(S3Acl.GRANT_READ);
    String grantWrites = getHeaders().getHeaderString(S3Acl.GRANT_WRITE);
    String grantReadACP = getHeaders().getHeaderString(S3Acl.GRANT_READ_ACP);
    String grantWriteACP = getHeaders().getHeaderString(S3Acl.GRANT_WRITE_ACP);
    String grantFull = getHeaders().getHeaderString(S3Acl.GRANT_FULL_CONTROL);

    try {
      OzoneBucket bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      OzoneVolume volume = getVolume();

      List<OzoneAcl> ozoneAclListOnBucket = new ArrayList<>();
      List<OzoneAcl> ozoneAclListOnVolume = new ArrayList<>();

      if (grantReads == null && grantWrites == null && grantReadACP == null
          && grantWriteACP == null && grantFull == null) {
        // Handle grants in body
        S3BucketAcl putBucketAclRequest =
            new PutBucketAclRequestUnmarshaller().readFrom(body);
        ozoneAclListOnBucket.addAll(
            S3Acl.s3AclToOzoneNativeAclOnBucket(putBucketAclRequest));
        ozoneAclListOnVolume.addAll(
            S3Acl.s3AclToOzoneNativeAclOnVolume(putBucketAclRequest));
      } else {
        // Handle grants in headers
        if (grantReads != null) {
          ozoneAclListOnBucket.addAll(getAndConvertAclOnBucket(grantReads,
              S3Acl.ACLType.READ.getValue()));
          ozoneAclListOnVolume.addAll(getAndConvertAclOnVolume(grantReads,
              S3Acl.ACLType.READ.getValue()));
        }
        if (grantWrites != null) {
          ozoneAclListOnBucket.addAll(getAndConvertAclOnBucket(grantWrites,
              S3Acl.ACLType.WRITE.getValue()));
          ozoneAclListOnVolume.addAll(getAndConvertAclOnVolume(grantWrites,
              S3Acl.ACLType.WRITE.getValue()));
        }
        if (grantReadACP != null) {
          ozoneAclListOnBucket.addAll(getAndConvertAclOnBucket(grantReadACP,
              S3Acl.ACLType.READ_ACP.getValue()));
          ozoneAclListOnVolume.addAll(getAndConvertAclOnVolume(grantReadACP,
              S3Acl.ACLType.READ_ACP.getValue()));
        }
        if (grantWriteACP != null) {
          ozoneAclListOnBucket.addAll(getAndConvertAclOnBucket(grantWriteACP,
              S3Acl.ACLType.WRITE_ACP.getValue()));
          ozoneAclListOnVolume.addAll(getAndConvertAclOnVolume(grantWriteACP,
              S3Acl.ACLType.WRITE_ACP.getValue()));
        }
        if (grantFull != null) {
          ozoneAclListOnBucket.addAll(getAndConvertAclOnBucket(grantFull,
              S3Acl.ACLType.FULL_CONTROL.getValue()));
          ozoneAclListOnVolume.addAll(getAndConvertAclOnVolume(grantFull,
              S3Acl.ACLType.FULL_CONTROL.getValue()));
        }
      }

      // A put request will reset all previous ACLs on bucket
      bucket.setAcl(ozoneAclListOnBucket);

      // A put request will reset input user/group's permission on volume
      List<OzoneAcl> acls = bucket.getAcls();
      List<OzoneAcl> aclsToRemoveOnVolume = new ArrayList<>();
      List<OzoneAcl> currentAclsOnVolume = volume.getAcls();

      // Remove input user/group's permission from Volume first
      if (!currentAclsOnVolume.isEmpty()) {
        for (OzoneAcl acl : acls) {
          if (acl.getAclScope() == ACCESS) {
            aclsToRemoveOnVolume.addAll(OzoneAclUtil.filterAclList(
                acl.getName(), acl.getType(), currentAclsOnVolume));
          }
        }
        for (OzoneAcl acl : aclsToRemoveOnVolume) {
          volume.removeAcl(acl);
        }
      }

      // Add new permission on Volume
      for (OzoneAcl acl : ozoneAclListOnVolume) {
        volume.addAcl(acl);
      }

      getMetrics().updatePutAclSuccessStats(startNanos);
      auditWriteSuccess(s3GAction);
      return Response.status(HttpStatus.SC_OK).build();

    } catch (OMException exception) {
      getMetrics().updatePutAclFailureStats(startNanos);
      auditWriteFailure(s3GAction, exception);
      if (exception.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, exception);
      } else if (isAccessDenied(exception)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, exception);
      }
      throw exception;
    } catch (OS3Exception ex) {
      getMetrics().updatePutAclFailureStats(startNanos);
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }
  }

  /**
   * Convert ACL string to Ozone ACL on bucket.
   *
   * Example: x-amz-grant-write: id="111122223333", id="555566667777"
   */
  private List<OzoneAcl> getAndConvertAclOnBucket(String value,
                                                  String permission)
      throws OS3Exception {
    return parseAndConvertAcl(value, permission, true);
  }

  /**
   * Convert ACL string to Ozone ACL on volume.
   */
  private List<OzoneAcl> getAndConvertAclOnVolume(String value,
                                                  String permission)
      throws OS3Exception {
    return parseAndConvertAcl(value, permission, false);
  }

  /**
   * Parse ACL string and convert to Ozone ACLs.
   *
   * This is a common method extracted from getAndConvertAclOnBucket and
   * getAndConvertAclOnVolume to reduce code duplication.
   *
   * @param value the ACL header value (e.g., "id=\"user1\",id=\"user2\"")
   * @param permission the S3 permission type (READ, WRITE, etc.)
   * @param isBucket true for bucket ACL, false for volume ACL
   * @return list of OzoneAcl objects
   * @throws OS3Exception if parsing fails or grantee type is not supported
   */
  private List<OzoneAcl> parseAndConvertAcl(String value, String permission,
                                            boolean isBucket)
      throws OS3Exception {
    List<OzoneAcl> ozoneAclList = new ArrayList<>();
    if (StringUtils.isEmpty(value)) {
      return ozoneAclList;
    }

    String[] subValues = value.split(",");
    for (String acl : subValues) {
      String[] part = acl.split("=");
      if (part.length != 2) {
        throw newError(S3ErrorTable.INVALID_ARGUMENT, acl);
      }

      S3Acl.ACLIdentityType type =
          S3Acl.ACLIdentityType.getTypeFromHeaderType(part[0]);
      if (type == null || !type.isSupported()) {
        LOG.warn("S3 grantee {} is null or not supported", part[0]);
        throw newError(NOT_IMPLEMENTED, part[0]);
      }

      String userId = part[1];

      if (isBucket) {
        // Build ACL on Bucket
        EnumSet<IAccessAuthorizer.ACLType> aclsOnBucket =
            S3Acl.getOzoneAclOnBucketFromS3Permission(permission);
        ozoneAclList.add(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER,
            userId,
            OzoneAcl.AclScope.DEFAULT,
            aclsOnBucket
        ));
        ozoneAclList.add(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER,
            userId,
            ACCESS,
            aclsOnBucket
        ));
      } else {
        // Build ACL on Volume
        EnumSet<IAccessAuthorizer.ACLType> aclsOnVolume =
            S3Acl.getOzoneAclOnVolumeFromS3Permission(permission);
        ozoneAclList.add(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER,
            userId,
            ACCESS,
            aclsOnVolume
        ));
      }
    }

    return ozoneAclList;
  }

  @Override
  @PostConstruct
  public void init() {
    // No initialization needed for AclHandler
  }
}
