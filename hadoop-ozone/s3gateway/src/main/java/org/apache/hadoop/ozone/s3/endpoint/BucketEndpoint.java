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

import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIST_KEYS_SHALLOW_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIST_KEYS_SHALLOW_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIST_MAX_KEYS_LIMIT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIST_MAX_KEYS_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.DeletedObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.Error;
import org.apache.hadoop.ozone.s3.endpoint.S3BucketAcl.Grant;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.ContinueToken;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket level rest endpoints.
 */
@Path("/{bucket}")
public class BucketEndpoint extends EndpointBase {

  private static final String BUCKET = "bucket";

  private static final Logger LOG =
      LoggerFactory.getLogger(BucketEndpoint.class);

  private boolean listKeysShallowEnabled;
  private int maxKeysLimit = 1000;

  private static final List<BucketOperationHandler> PUT_HANDLERS =
      Collections.unmodifiableList(Arrays.asList(
          new AclHandler()
      ));

  /**
   * Rest endpoint to list objects in a specific bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
   * for more details.
   */
  @GET
  @SuppressWarnings("methodlength")
  public Response get(
      @PathParam(BUCKET) String bucketName
  ) throws OS3Exception, IOException {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.GET_BUCKET;
    PerformanceStringBuilder perf = new PerformanceStringBuilder();

    final String continueToken = queryParams().get(QueryParams.CONTINUATION_TOKEN);
    final String delimiter = queryParams().get(QueryParams.DELIMITER);
    final String encodingType = queryParams().get(QueryParams.ENCODING_TYPE);
    final String marker = queryParams().get(QueryParams.MARKER);
    int maxKeys = queryParams().getInt(QueryParams.MAX_KEYS, 1000);
    final int maxUploads = queryParams().getInt(QueryParams.MAX_UPLOADS, 1000);
    String prefix = queryParams().get(QueryParams.PREFIX);
    String startAfter = queryParams().get(QueryParams.START_AFTER);

    Iterator<? extends OzoneKey> ozoneKeyIterator = null;
    ContinueToken decodedToken =
        ContinueToken.decodeFromString(continueToken);
    OzoneBucket bucket = null;

    try {
      final String aclMarker = queryParams().get(QueryParams.ACL);
      if (aclMarker != null) {
        s3GAction = S3GAction.GET_ACL;
        S3BucketAcl result = getAcl(bucketName);
        getMetrics().updateGetAclSuccessStats(startNanos);
        auditReadSuccess(s3GAction);
        return Response.ok(result, MediaType.APPLICATION_XML_TYPE).build();
      }

      final String uploads = queryParams().get(QueryParams.UPLOADS);
      if (uploads != null) {
        s3GAction = S3GAction.LIST_MULTIPART_UPLOAD;
        final String uploadIdMarker = queryParams().get(QueryParams.UPLOAD_ID_MARKER);
        final String keyMarker = queryParams().get(QueryParams.KEY_MARKER);
        return listMultipartUploads(bucketName, prefix, keyMarker, uploadIdMarker, maxUploads);
      }

      maxKeys = validateMaxKeys(maxKeys);

      if (prefix == null) {
        prefix = "";
      }

      // Assign marker to startAfter. for the compatibility of aws api v1
      if (startAfter == null && marker != null) {
        startAfter = marker;
      }

      // If continuation token and start after both are provided, then we
      // ignore start After
      String prevKey = continueToken != null ? decodedToken.getLastKey()
          : startAfter;

      // If shallow is true, only list immediate children
      // delimited by OZONE_URI_DELIMITER
      boolean shallow = listKeysShallowEnabled
          && OZONE_URI_DELIMITER.equals(delimiter);

      bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());

      ozoneKeyIterator = bucket.listKeys(prefix, prevKey, shallow);

    } catch (OMException ex) {
      auditReadFailure(s3GAction, ex);
      getMetrics().updateGetBucketFailureStats(startNanos);
      if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.FILE_NOT_FOUND) {
        // File not found, continue and send normal response with 0 keyCount
        LOG.debug("Key Not found prefix: {}", prefix);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      getMetrics().updateGetBucketFailureStats(startNanos);
      auditReadFailure(s3GAction, ex);
      throw ex;
    }

    // The valid encodingType Values is "url"
    if (encodingType != null && !encodingType.equals(ENCODING_TYPE)) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, encodingType);
    }

    // If you specify the encoding-type request parameter,should return
    // encoded key name values in the following response elements:
    //   Delimiter, Prefix, Key, and StartAfter.
    //
    // For detail refer:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    // #AmazonS3-ListObjectsV2-response-EncodingType
    //
    ListObjectResponse response = new ListObjectResponse();
    response.setDelimiter(
        EncodingTypeObject.createNullable(delimiter, encodingType));
    response.setName(bucketName);
    response.setPrefix(EncodingTypeObject.createNullable(prefix, encodingType));
    response.setMarker(marker == null ? "" : marker);
    response.setMaxKeys(maxKeys);
    response.setEncodingType(encodingType);
    response.setTruncated(false);
    response.setContinueToken(continueToken);
    response.setStartAfter(
        EncodingTypeObject.createNullable(startAfter, encodingType));

    String prevDir = null;
    if (continueToken != null) {
      prevDir = decodedToken.getLastDir();
    }
    String lastKey = null;
    int count = 0;
    if (maxKeys > 0) {
      while (ozoneKeyIterator != null && ozoneKeyIterator.hasNext()) {
        OzoneKey next = ozoneKeyIterator.next();
        if (bucket != null && bucket.getBucketLayout().isFileSystemOptimized() &&
            StringUtils.isNotEmpty(prefix) &&
            !next.getName().startsWith(prefix)) {
          // prefix has delimiter but key don't have
          // example prefix: dir1/ key: dir123
          continue;
        }
        if (startAfter != null && count == 0 && Objects.equals(startAfter, next.getName())) {
          continue;
        }
        String relativeKeyName = next.getName().substring(prefix.length());

        int depth = StringUtils.countMatches(relativeKeyName, delimiter);
        if (!StringUtils.isEmpty(delimiter)) {
          if (depth > 0) {
            // means key has multiple delimiters in its value.
            // ex: dir/dir1/dir2, where delimiter is "/" and prefix is dir/
            String dirName = relativeKeyName.substring(0, relativeKeyName
                .indexOf(delimiter));
            if (!dirName.equals(prevDir)) {
              response.addPrefix(EncodingTypeObject.createNullable(
                  prefix + dirName + delimiter, encodingType));
              prevDir = dirName;
              count++;
            }
          } else if (relativeKeyName.endsWith(delimiter)) {
            // means or key is same as prefix with delimiter at end and ends with
            // delimiter. ex: dir/, where prefix is dir and delimiter is /
            response.addPrefix(
                EncodingTypeObject.createNullable(relativeKeyName, encodingType));
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
    }

    response.setKeyCount(count);

    if (count < maxKeys) {
      response.setTruncated(false);
    } else if (ozoneKeyIterator.hasNext() && lastKey != null) {
      response.setTruncated(true);
      ContinueToken nextToken = new ContinueToken(lastKey, prevDir);
      response.setNextToken(nextToken.encodeToString());
      // Set nextMarker to be lastKey. for the compatibility of aws api v1
      response.setNextMarker(lastKey);
    } else {
      response.setTruncated(false);
    }

    int keyCount =
        response.getCommonPrefixes().size() + response.getContents().size();
    long opLatencyNs =
        getMetrics().updateGetBucketSuccessStats(startNanos);
    getMetrics().incListKeyCount(keyCount);
    perf.appendCount(keyCount);
    perf.appendOpLatencyNanos(opLatencyNs);
    auditReadSuccess(s3GAction, perf);
    response.setKeyCount(keyCount);
    return Response.ok(response).build();
  }

  private int validateMaxKeys(int maxKeys) throws OS3Exception {
    if (maxKeys < 0) {
      throw newError(S3ErrorTable.INVALID_ARGUMENT, "maxKeys must be >= 0");
    }

    return Math.min(maxKeys, maxKeysLimit);
  }

  @PUT
  public Response put(
      @PathParam(BUCKET) String bucketName,
      InputStream body
  ) throws IOException, OS3Exception {

    // Chain of responsibility: let each handler try to handle the request
    for (BucketOperationHandler handler : PUT_HANDLERS) {
      Response response = handler.handlePutRequest(bucketName, body);
      if (response != null) {
        return response;  // Handler handled the request
      }
    }

    // No handler handled the request, execute default operation: create bucket
    return handleCreateBucket(bucketName);
  }

  /**
   * Default PUT bucket operation (create bucket).
   */
  private Response handleCreateBucket(String bucketName)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.CREATE_BUCKET;

    try {
      String location = createS3Bucket(bucketName);
      auditWriteSuccess(s3GAction);
      getMetrics().updateCreateBucketSuccessStats(startNanos);
      return Response.status(HttpStatus.SC_OK).header("Location", location)
          .build();
    } catch (OMException exception) {
      auditWriteFailure(s3GAction, exception);
      getMetrics().updateCreateBucketFailureStats(startNanos);
      if (exception.getResult() == ResultCodes.INVALID_BUCKET_NAME) {
        throw newError(S3ErrorTable.INVALID_BUCKET_NAME, bucketName, exception);
      }
      throw exception;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }
  }

  public Response listMultipartUploads(
      String bucketName,
      String prefix,
      String keyMarker,
      String uploadIdMarker,
      int maxUploads)
      throws OS3Exception, IOException {

    if (maxUploads < 1) {
      throw newError(S3ErrorTable.INVALID_ARGUMENT, "max-uploads",
          new Exception("max-uploads must be positive"));
    } else {
      maxUploads = Math.min(maxUploads, 1000);
    }

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.LIST_MULTIPART_UPLOAD;

    OzoneBucket bucket = getBucket(bucketName);

    try {
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      OzoneMultipartUploadList ozoneMultipartUploadList =
          bucket.listMultipartUploads(prefix, keyMarker, uploadIdMarker, maxUploads);

      ListMultipartUploadsResult result = new ListMultipartUploadsResult();
      result.setBucket(bucketName);
      result.setKeyMarker(keyMarker);
      result.setUploadIdMarker(uploadIdMarker);
      result.setNextKeyMarker(ozoneMultipartUploadList.getNextKeyMarker());
      result.setPrefix(prefix);
      result.setNextUploadIdMarker(ozoneMultipartUploadList.getNextUploadIdMarker());
      result.setMaxUploads(maxUploads);
      result.setTruncated(ozoneMultipartUploadList.isTruncated());

      ozoneMultipartUploadList.getUploads().forEach(upload -> result.addUpload(
          new ListMultipartUploadsResult.Upload(
              upload.getKeyName(),
              upload.getUploadId(),
              upload.getCreationTime(),
              S3StorageType.fromReplicationConfig(upload.getReplicationConfig())
          )));
      auditReadSuccess(s3GAction);
      getMetrics().updateListMultipartUploadsSuccessStats(startNanos);
      return Response.ok(result).build();
    } catch (OMException exception) {
      auditReadFailure(s3GAction, exception);
      getMetrics().updateListMultipartUploadsFailureStats(startNanos);
      if (isAccessDenied(exception)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, prefix, exception);
      }
      throw exception;
    } catch (Exception ex) {
      auditReadFailure(s3GAction, ex);
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
  public Response head(@PathParam(BUCKET) String bucketName)
      throws OS3Exception, IOException {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.HEAD_BUCKET;
    try {
      OzoneBucket bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      auditReadSuccess(s3GAction);
      getMetrics().updateHeadBucketSuccessStats(startNanos);
      return Response.ok().build();
    } catch (Exception e) {
      auditReadFailure(s3GAction, e);
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
  public Response delete(@PathParam(BUCKET) String bucketName)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.DELETE_BUCKET;

    try {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }
      deleteS3Bucket(bucketName);
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateDeleteBucketFailureStats(startNanos);
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
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }

    auditWriteSuccess(s3GAction);
    getMetrics().updateDeleteBucketSuccessStats(startNanos);
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
  public MultiDeleteResponse multiDelete(
      @PathParam(BUCKET) String bucketName,
      @QueryParam(QueryParams.DELETE) String delete,
      MultiDeleteRequest request
  ) throws OS3Exception, IOException {
    S3GAction s3GAction = S3GAction.MULTI_DELETE;

    OzoneBucket bucket = getBucket(bucketName);
    MultiDeleteResponse result = new MultiDeleteResponse();
    List<String> deleteKeys = new ArrayList<>();

    if (request.getObjects() != null) {
      Map<String, ErrorInfo> undeletedKeyResultMap;
      for (DeleteObject keyToDelete : request.getObjects()) {
        deleteKeys.add(keyToDelete.getKey());
      }
      long startNanos = Time.monotonicNowNanos();
      try {
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
        undeletedKeyResultMap = bucket.deleteKeys(deleteKeys, true);
        for (DeleteObject d : request.getObjects()) {
          ErrorInfo error = undeletedKeyResultMap.get(d.getKey());
          boolean deleted = error == null ||
              // if the key is not found, it is assumed to be successfully deleted
              ResultCodes.KEY_NOT_FOUND.name().equals(error.getCode());
          if (deleted) {
            deleteKeys.remove(d.getKey());
            if (!request.isQuiet()) {
              result.addDeleted(new DeletedObject(d.getKey()));
            }
          } else {
            result.addError(new Error(d.getKey(), error.getCode(), error.getMessage()));
          }
        }
        getMetrics().updateDeleteKeySuccessStats(startNanos);
      } catch (IOException ex) {
        LOG.error("Delete key failed: {}", ex.getMessage());
        getMetrics().updateDeleteKeyFailureStats(startNanos);
        result.addError(
            new Error("ALL", "InternalError",
                ex.getMessage()));
      }
    }

    AuditMessage.Builder message = auditMessageFor(s3GAction);
    message.getParams().put("failedDeletes", deleteKeys.toString());

    if (!result.getErrors().isEmpty()) {
      AUDIT.logWriteFailure(message.withResult(AuditEventStatus.FAILURE)
          .withException(new Exception("MultiDelete Exception")).build());
    } else {
      AUDIT.logWriteSuccess(message.withResult(AuditEventStatus.SUCCESS).build());
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
    long startNanos = Time.monotonicNowNanos();
    S3BucketAcl result = new S3BucketAcl();
    try {
      OzoneBucket bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      S3Owner owner = S3Owner.of(bucket.getOwner());
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
      getMetrics().updateGetAclFailureStats(startNanos);
      auditReadFailure(S3GAction.GET_ACL, ex);
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName, ex);
      } else {
        throw newError(S3ErrorTable.INTERNAL_ERROR, bucketName, ex);
      }
    } catch (OS3Exception ex) {
      getMetrics().updateGetAclFailureStats(startNanos);
      throw ex;
    }
  }

  private void addKey(ListObjectResponse response, OzoneKey next) {
    KeyMetadata keyMetadata = new KeyMetadata();
    keyMetadata.setKey(EncodingTypeObject.createNullable(next.getName(),
        response.getEncodingType()));
    keyMetadata.setSize(next.getDataSize());
    String eTag = next.getMetadata().get(ETAG);
    if (eTag != null) {
      keyMetadata.setETag(wrapInQuotes(eTag));
    }
    keyMetadata.setStorageClass(S3StorageType.fromReplicationConfig(
        next.getReplicationConfig()).toString());
    keyMetadata.setLastModified(next.getModificationTime());
    String displayName = next.getOwner();
    keyMetadata.setOwner(S3Owner.of(displayName));
    response.addKey(keyMetadata);
  }

  @Override
  @PostConstruct
  public void init() {
    listKeysShallowEnabled = getOzoneConfiguration().getBoolean(
        OZONE_S3G_LIST_KEYS_SHALLOW_ENABLED,
        OZONE_S3G_LIST_KEYS_SHALLOW_ENABLED_DEFAULT);
    maxKeysLimit = getOzoneConfiguration().getInt(
        OZONE_S3G_LIST_MAX_KEYS_LIMIT,
        OZONE_S3G_LIST_MAX_KEYS_LIMIT_DEFAULT);
  }
}
