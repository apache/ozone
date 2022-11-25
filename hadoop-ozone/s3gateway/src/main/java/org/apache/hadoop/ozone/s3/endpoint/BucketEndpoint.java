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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OzoneAclUtil;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.DeletedObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.Error;
import org.apache.hadoop.ozone.s3.endpoint.S3BucketAcl.Grant;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.ContinueToken;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;

/**
 * Bucket level rest endpoints.
 */
@Path("/{bucket}")
public class BucketEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BucketEndpoint.class);

  /**
   * Rest endpoint to list objects in a specific bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
   * for more details.
   */
  @GET
  @SuppressFBWarnings
  @SuppressWarnings({"parameternumber", "methodlength"})
  public Response get(
      @PathParam("bucket") String bucketName,
      @QueryParam("delimiter") String delimiter,
      @QueryParam("encoding-type") String encodingType,
      @QueryParam("marker") String marker,
      @DefaultValue("1000") @QueryParam("max-keys") int maxKeys,
      @QueryParam("prefix") String prefix,
      @QueryParam("continuation-token") String continueToken,
      @QueryParam("start-after") String startAfter,
      @QueryParam("uploads") String uploads,
      @QueryParam("acl") String aclMarker,
      @Context HttpHeaders hh) throws OS3Exception, IOException {
    S3GAction s3GAction = S3GAction.GET_BUCKET;
    Iterator<? extends OzoneKey> ozoneKeyIterator;
    ContinueToken decodedToken =
        ContinueToken.decodeFromString(continueToken);

    try {
      if (aclMarker != null) {
        s3GAction = S3GAction.GET_ACL;
        S3BucketAcl result = getAcl(bucketName);
        getMetrics().incGetAclSuccess();
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
        return Response.ok(result, MediaType.APPLICATION_XML_TYPE).build();
      }

      if (uploads != null) {
        s3GAction = S3GAction.LIST_MULTIPART_UPLOAD;
        return listMultipartUploads(bucketName, prefix);
      }

      if (prefix == null) {
        prefix = "";
      }

      // Assign marker to startAfter. for the compatibility of aws api v1
      if (startAfter == null && marker != null) {
        startAfter = marker;
      }
      
      OzoneBucket bucket = getBucket(bucketName);
      if (startAfter != null && continueToken != null) {
        // If continuation token and start after both are provided, then we
        // ignore start After
        ozoneKeyIterator = bucket.listKeys(prefix, decodedToken.getLastKey());
      } else if (startAfter != null && continueToken == null) {
        ozoneKeyIterator = bucket.listKeys(prefix, startAfter);
      } else if (startAfter == null && continueToken != null) {
        ozoneKeyIterator = bucket.listKeys(prefix, decodedToken.getLastKey());
      } else {
        ozoneKeyIterator = bucket.listKeys(prefix);
      }
    } catch (OMException ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      getMetrics().incGetBucketFailure();
      if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      getMetrics().incGetBucketFailure();
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      throw ex;
    }

    ListObjectResponse response = new ListObjectResponse();
    response.setDelimiter(delimiter);
    response.setName(bucketName);
    response.setPrefix(prefix);
    response.setMarker(marker == null ? "" : marker);
    response.setMaxKeys(maxKeys);
    response.setEncodingType(ENCODING_TYPE);
    response.setTruncated(false);
    response.setContinueToken(continueToken);

    String prevDir = null;
    if (continueToken != null) {
      prevDir = decodedToken.getLastDir();
    }
    String lastKey = null;
    int count = 0;
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey next = ozoneKeyIterator.next();
      String relativeKeyName = next.getName().substring(prefix.length());

      int depth = StringUtils.countMatches(relativeKeyName, delimiter);
      if (delimiter != null) {
        if (depth > 0) {
          // means key has multiple delimiters in its value.
          // ex: dir/dir1/dir2, where delimiter is "/" and prefix is dir/
          String dirName = relativeKeyName.substring(0, relativeKeyName
              .indexOf(delimiter));
          if (!dirName.equals(prevDir)) {
            response.addPrefix(prefix + dirName + delimiter);
            prevDir = dirName;
            count++;
          }
        } else if (relativeKeyName.endsWith(delimiter)) {
          // means or key is same as prefix with delimiter at end and ends with
          // delimiter. ex: dir/, where prefix is dir and delimiter is /
          response.addPrefix(relativeKeyName);
          count++;
        } else {
          // means our key is matched with prefix if prefix is given and it
          // does not have any common prefix.
          addKey(response, next);
          count++;
        }
      } else {
        addKey(response, next);
        count++;
      }

      if (count == maxKeys) {
        lastKey = next.getName();
        break;
      }
    }

    response.setKeyCount(count);

    if (count < maxKeys) {
      response.setTruncated(false);
    } else if (ozoneKeyIterator.hasNext()) {
      response.setTruncated(true);
      ContinueToken nextToken = new ContinueToken(lastKey, prevDir);
      response.setNextToken(nextToken.encodeToString());
      // Set nextMarker to be lastKey. for the compatibility of aws api v1
      response.setNextMarker(lastKey);
    } else {
      response.setTruncated(false);
    }

    AUDIT.logReadSuccess(buildAuditMessageForSuccess(s3GAction,
        getAuditParameters()));
    getMetrics().incGetBucketSuccess();
    response.setKeyCount(
        response.getCommonPrefixes().size() + response.getContents().size());
    return Response.ok(response).build();
  }

  @PUT
  public Response put(@PathParam("bucket") String bucketName,
                      @QueryParam("acl") String aclMarker,
                      @Context HttpHeaders httpHeaders,
                      InputStream body) throws IOException, OS3Exception {
    S3GAction s3GAction = S3GAction.CREATE_BUCKET;

    try {
      if (aclMarker != null) {
        s3GAction = S3GAction.PUT_ACL;
        Response response =  putAcl(bucketName, httpHeaders, body);
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
        return response;
      }
      String location = createS3Bucket(bucketName);
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      getMetrics().incCreateBucketSuccess();
      return Response.status(HttpStatus.SC_OK).header("Location", location)
          .build();
    } catch (OMException exception) {
      auditWriteFailure(s3GAction, exception);
      getMetrics().incCreateBucketFailure();
      if (exception.getResult() == ResultCodes.INVALID_BUCKET_NAME) {
        throw newError(S3ErrorTable.INVALID_BUCKET_NAME, bucketName, exception);
      }
      throw exception;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      throw ex;
    }
  }

  public Response listMultipartUploads(
      @PathParam("bucket") String bucketName,
      @QueryParam("prefix") String prefix)
      throws OS3Exception, IOException {
    S3GAction s3GAction = S3GAction.LIST_MULTIPART_UPLOAD;

    OzoneBucket bucket = getBucket(bucketName);

    try {
      OzoneMultipartUploadList ozoneMultipartUploadList =
          bucket.listMultipartUploads(prefix);

      ListMultipartUploadsResult result = new ListMultipartUploadsResult();
      result.setBucket(bucketName);

      ozoneMultipartUploadList.getUploads().forEach(upload -> result.addUpload(
          new ListMultipartUploadsResult.Upload(
              upload.getKeyName(),
              upload.getUploadId(),
              upload.getCreationTime(),
              S3StorageType.fromReplicationConfig(upload.getReplicationConfig())
          )));
      AUDIT.logReadSuccess(buildAuditMessageForSuccess(s3GAction,
          getAuditParameters()));
      getMetrics().incListMultipartUploadsSuccess();
      return Response.ok(result).build();
    } catch (OMException exception) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(),
              exception));
      getMetrics().incListMultipartUploadsFailure();
      if (isAccessDenied(exception)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, prefix, exception);
      }
      throw exception;
    } catch (Exception ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      throw ex;
    }
  }

  /**
   * Rest endpoint to check the existence of a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(@PathParam("bucket") String bucketName)
      throws OS3Exception, IOException {
    S3GAction s3GAction = S3GAction.HEAD_BUCKET;
    try {
      getBucket(bucketName);
      AUDIT.logReadSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      getMetrics().incHeadBucketSuccess();
      return Response.ok().build();
    } catch (Exception e) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), e));
      throw e;
    }
  }

  /**
   * Rest endpoint to delete specific bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETE.html
   * for more details.
   */
  @DELETE
  public Response delete(@PathParam("bucket") String bucketName)
      throws IOException, OS3Exception {
    S3GAction s3GAction = S3GAction.DELETE_BUCKET;

    try {
      deleteS3Bucket(bucketName);
    } catch (OMException ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      getMetrics().incDeleteBucketFailure();
      if (ex.getResult() == ResultCodes.BUCKET_NOT_EMPTY) {
        throw newError(S3ErrorTable.BUCKET_NOT_EMPTY, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      throw ex;
    }

    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(s3GAction,
        getAuditParameters()));
    getMetrics().incDeleteBucketSuccess();
    return Response
        .status(HttpStatus.SC_NO_CONTENT)
        .build();

  }

  /**
   * Implement multi delete.
   * <p>
   * see: https://docs.aws.amazon
   * .com/AmazonS3/latest/API/multiobjectdeleteapi.html
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public MultiDeleteResponse multiDelete(@PathParam("bucket") String bucketName,
                                         @QueryParam("delete") String delete,
                                         MultiDeleteRequest request)
      throws OS3Exception, IOException {
    S3GAction s3GAction = S3GAction.MULTI_DELETE;

    OzoneBucket bucket = getBucket(bucketName);
    MultiDeleteResponse result = new MultiDeleteResponse();
    if (request.getObjects() != null) {
      for (DeleteObject keyToDelete : request.getObjects()) {
        try {
          bucket.deleteKey(keyToDelete.getKey());

          if (!request.isQuiet()) {
            result.addDeleted(new DeletedObject(keyToDelete.getKey()));
          }
        } catch (OMException ex) {
          if (isAccessDenied(ex)) {
            result.addError(
                new Error(keyToDelete.getKey(), "PermissionDenied",
                    ex.getMessage()));
          } else if (ex.getResult() != ResultCodes.KEY_NOT_FOUND) {
            result.addError(
                new Error(keyToDelete.getKey(), "InternalError",
                    ex.getMessage()));
          } else if (!request.isQuiet()) {
            result.addDeleted(new DeletedObject(keyToDelete.getKey()));
          }
        } catch (Exception ex) {
          result.addError(
              new Error(keyToDelete.getKey(), "InternalError",
                  ex.getMessage()));
        }
      }
    }
    if (result.getErrors().size() != 0) {
      AUDIT.logWriteFailure(buildAuditMessageForFailure(s3GAction,
          getAuditParameters(), new Exception("MultiDelete Exception")));
    } else {
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
    }
    return result;
  }

  /**
   * Implement acl get.
   * <p>
   * see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
   */
  public S3BucketAcl getAcl(String bucketName)
      throws OS3Exception, IOException {
    S3BucketAcl result = new S3BucketAcl();
    try {
      OzoneBucket bucket = getBucket(bucketName);
      OzoneVolume volume = getVolume();
      // TODO: use bucket owner instead of volume owner here once bucket owner
      // TODO: is supported.
      S3Owner owner = new S3Owner(volume.getOwner(), volume.getOwner());
      result.setOwner(owner);

      // TODO: remove this duplication avoid logic when ACCESS and DEFAULT scope
      // TODO: are merged.
      // Use set to remove ACLs with different scopes(ACCESS and DEFAULT)
      Set<Grant> grantSet = new HashSet<>();
      // Return ACL list
      for (OzoneAcl acl : bucket.getAcls()) {
        List<Grant> grants = S3Acl.ozoneNativeAclToS3Acl(acl);
        grantSet.addAll(grants);
      }
      ArrayList<Grant> grantList = new ArrayList<>();
      grantList.addAll(grantSet);
      result.setAclList(
          new S3BucketAcl.AccessControlList(grantList));
      return result;
    } catch (OMException ex) {
      getMetrics().incGetAclFailure();
      auditReadFailure(S3GAction.GET_ACL, ex);
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else {
        throw newError(S3ErrorTable.INTERNAL_ERROR, bucketName, ex);
      }
    } catch (OS3Exception ex) {
      getMetrics().incGetAclFailure();
      throw ex;
    }
  }

  /**
   * Implement acl put.
   * <p>
   * see: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
   */
  public Response putAcl(String bucketName, HttpHeaders httpHeaders,
                         InputStream body) throws IOException, OS3Exception {
    String grantReads = httpHeaders.getHeaderString(S3Acl.GRANT_READ);
    String grantWrites = httpHeaders.getHeaderString(S3Acl.GRANT_WRITE);
    String grantReadACP = httpHeaders.getHeaderString(S3Acl.GRANT_READ_CAP);
    String grantWriteACP = httpHeaders.getHeaderString(S3Acl.GRANT_WRITE_CAP);
    String grantFull = httpHeaders.getHeaderString(S3Acl.GRANT_FULL_CONTROL);

    try {
      OzoneBucket bucket = getBucket(bucketName);
      OzoneVolume volume = getVolume();

      List<OzoneAcl> ozoneAclListOnBucket = new ArrayList<>();
      List<OzoneAcl> ozoneAclListOnVolume = new ArrayList<>();

      if (grantReads == null && grantWrites == null && grantReadACP == null
          && grantWriteACP == null && grantFull == null) {
        S3BucketAcl putBucketAclRequest =
            new PutBucketAclRequestUnmarshaller().readFrom(
                null, null, null, null, null, body);
        // Handle grants in body
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
      if (currentAclsOnVolume.size() > 0) {
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
    } catch (OMException exception) {
      getMetrics().incPutAclFailure();
      auditWriteFailure(S3GAction.PUT_ACL, exception);
      if (exception.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, exception);
      } else if (isAccessDenied(exception)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, exception);
      }
      throw exception;
    } catch (OS3Exception ex) {
      getMetrics().incPutAclFailure();
      throw ex;
    }
    getMetrics().incPutAclSuccess();
    return Response.status(HttpStatus.SC_OK).build();
  }

  /**
   * Example: x-amz-grant-write: \
   * uri="http://acs.amazonaws.com/groups/s3/LogDelivery", id="111122223333", \
   * id="555566667777".
   */
  private List<OzoneAcl> getAndConvertAclOnBucket(String value,
                                                  String permission)
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
      // Build ACL on Bucket
      BitSet aclsOnBucket =
          S3Acl.getOzoneAclOnBucketFromS3Permission(permission);
      OzoneAcl defaultOzoneAcl = new OzoneAcl(
          IAccessAuthorizer.ACLIdentityType.USER, part[1], aclsOnBucket,
          OzoneAcl.AclScope.DEFAULT);
      OzoneAcl accessOzoneAcl = new OzoneAcl(
          IAccessAuthorizer.ACLIdentityType.USER, part[1], aclsOnBucket,
          ACCESS);
      ozoneAclList.add(defaultOzoneAcl);
      ozoneAclList.add(accessOzoneAcl);
    }
    return ozoneAclList;
  }

  private List<OzoneAcl> getAndConvertAclOnVolume(String value,
                                                  String permission)
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
      // Build ACL on Volume
      BitSet aclsOnVolume =
          S3Acl.getOzoneAclOnVolumeFromS3Permission(permission);
      OzoneAcl accessOzoneAcl = new OzoneAcl(
          IAccessAuthorizer.ACLIdentityType.USER, part[1], aclsOnVolume,
          ACCESS);
      ozoneAclList.add(accessOzoneAcl);
    }
    return ozoneAclList;
  }

  private void addKey(ListObjectResponse response, OzoneKey next) {
    KeyMetadata keyMetadata = new KeyMetadata();
    keyMetadata.setKey(next.getName());
    keyMetadata.setSize(next.getDataSize());
    keyMetadata.setETag("" + next.getModificationTime());
    if (next.getReplicationType().toString().equals(ReplicationType
        .STAND_ALONE.toString())) {
      keyMetadata.setStorageClass(S3StorageType.REDUCED_REDUNDANCY.toString());
    } else {
      keyMetadata.setStorageClass(S3StorageType.STANDARD.toString());
    }
    keyMetadata.setLastModified(next.getModificationTime());
    response.addKey(keyMetadata);
  }

  @Override
  public void init() {

  }
}
