/*
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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.RangeHeaderParserUtil;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;

import com.google.common.collect.ImmutableMap;
import com.google.common.annotations.VisibleForTesting;
import static javax.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static javax.ws.rs.core.HttpHeaders.LAST_MODIFIED;
import org.apache.commons.io.IOUtils;

import org.apache.commons.lang3.tuple.Pair;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ENTITY_TOO_SMALL;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCEPT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CONTENT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER_SUPPORTED_UNIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlDecode;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key level rest endpoints.
 */
@Path("/{bucket}/{path:.+}")
public class ObjectEndpoint extends EndpointBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpoint.class);

  @Context
  private ContainerRequestContext context;

  @Context
  private HttpHeaders headers;

  /*FOR the feature Overriding Response Header
  https://docs.aws.amazon.com/de_de/AmazonS3/latest/API/API_GetObject.html */
  private Map<String, String> overrideQueryParameter;
  private int bufferSize;

  public ObjectEndpoint() {
    overrideQueryParameter = ImmutableMap.<String, String>builder()
        .put("Content-Type", "response-content-type")
        .put("Content-Language", "response-content-language")
        .put("Expires", "response-expires")
        .put("Cache-Control", "response-cache-control")
        .put("Content-Disposition", "response-content-disposition")
        .put("Content-Encoding", "response-content-encoding")
        .build();
  }

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @PostConstruct
  public void init() {
    bufferSize = (int) ozoneConfiguration.getStorageSize(
        OZONE_S3G_CLIENT_BUFFER_SIZE_KEY,
        OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  /**
   * Rest endpoint to upload object to a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html for
   * more details.
   */
  @PUT
  public Response put(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @HeaderParam("Content-Length") long length,
      @QueryParam("partNumber")  int partNumber,
      @QueryParam("uploadId") @DefaultValue("") String uploadID,
      InputStream body) throws IOException, OS3Exception {

    S3GAction s3GAction = S3GAction.CREATE_KEY;
    boolean auditSuccess = true;

    OzoneOutputStream output = null;

    String copyHeader = null, storageType = null;
    try {
      OzoneVolume volume = getVolume();
      if (uploadID != null && !uploadID.equals("")) {
        s3GAction = S3GAction.CREATE_MULTIPART_KEY;
        // If uploadID is specified, it is a request for upload part
        return createMultipartKey(volume, bucketName, keyPath, length,
            partNumber, uploadID, body);
      }

      copyHeader = headers.getHeaderString(COPY_SOURCE_HEADER);
      storageType = headers.getHeaderString(STORAGE_CLASS_HEADER);
      boolean storageTypeDefault = StringUtils.isEmpty(storageType);

      // Normal put object
      OzoneBucket bucket = volume.getBucket(bucketName);
      ReplicationConfig replicationConfig =
          getReplicationConfig(bucket, storageType);

      if (copyHeader != null) {
        //Copy object, as copy source available.
        s3GAction = S3GAction.COPY_OBJECT;
        CopyObjectResponse copyObjectResponse = copyObject(volume,
            copyHeader, bucketName, keyPath, replicationConfig,
            storageTypeDefault);
        return Response.status(Status.OK).entity(copyObjectResponse).header(
            "Connection", "close").build();
      }

      if ("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
          .equals(headers.getHeaderString("x-amz-content-sha256"))) {
        body = new SignedChunksInputStream(body);
      }

      output = getClientProtocol().createKey(volume.getName(), bucketName,
          keyPath, length, replicationConfig, new HashMap<>());
      IOUtils.copy(body, output);

      getMetrics().incCreateKeySuccess();
      return Response.ok().status(HttpStatus.SC_OK)
          .build();
    } catch (OMException ex) {
      auditSuccess = false;
      auditWriteFailure(s3GAction, ex);
      if (copyHeader != null) {
        getMetrics().incCopyObjectFailure();
      } else {
        getMetrics().incCreateKeyFailure();
      }
      if (ex.getResult() == ResultCodes.NOT_A_FILE) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, keyPath, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the PutObject/MPU PartUpload operation: " +
            OZONE_OM_ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
            " considered as Unix Paths. Path has Violated FS Semantics " +
            "which caused put operation to fail.");
        throw os3Exception;
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditSuccess = false;
      auditWriteFailure(s3GAction, ex);
      if (copyHeader != null) {
        getMetrics().incCopyObjectFailure();
      } else {
        getMetrics().incCreateKeyFailure();
      }
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logWriteSuccess(
            buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      }
      if (output != null) {
        output.close();
      }
    }
  }

  /**
   * Rest endpoint to download object from a bucket, if query param uploadId
   * is specified, request for list parts of a multipart upload key with
   * specific uploadId.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
   * https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * for more details.
   */
  @GET
  public Response get(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @QueryParam("uploadId") String uploadId,
      @QueryParam("max-parts") @DefaultValue("1000") int maxParts,
      @QueryParam("part-number-marker") String partNumberMarker,
      InputStream body) throws IOException, OS3Exception {

    S3GAction s3GAction = S3GAction.GET_KEY;
    boolean auditSuccess = true;

    try {
      if (uploadId != null) {
        // When we have uploadId, this is the request for list Parts.
        s3GAction = S3GAction.LIST_PARTS;
        int partMarker = parsePartNumberMarker(partNumberMarker);
        return listParts(bucketName, keyPath, uploadId,
            partMarker, maxParts);
      }

      OzoneVolume volume = getVolume();

      OzoneKeyDetails keyDetails = getClientProtocol().getKeyDetails(
          volume.getName(), bucketName, keyPath);

      long length = keyDetails.getDataSize();

      LOG.debug("Data length of the key {} is {}", keyPath, length);

      String rangeHeaderVal = headers.getHeaderString(RANGE_HEADER);
      RangeHeader rangeHeader = null;

      LOG.debug("range Header provided value: {}", rangeHeaderVal);

      if (rangeHeaderVal != null) {
        rangeHeader = RangeHeaderParserUtil.parseRangeHeader(rangeHeaderVal,
            length);
        LOG.debug("range Header provided: {}", rangeHeader);
        if (rangeHeader.isInValidRange()) {
          throw newError(S3ErrorTable.INVALID_RANGE, rangeHeaderVal);
        }
      }
      ResponseBuilder responseBuilder;

      if (rangeHeaderVal == null || rangeHeader.isReadFull()) {
        StreamingOutput output = dest -> {
          try (OzoneInputStream key = keyDetails.getContent()) {
            IOUtils.copy(key, dest);
          }
        };
        responseBuilder = Response
            .ok(output)
            .header(CONTENT_LENGTH, keyDetails.getDataSize());

      } else {

        long startOffset = rangeHeader.getStartOffset();
        long endOffset = rangeHeader.getEndOffset();
        // eg. if range header is given as bytes=0-0, then we should return 1
        // byte from start offset
        long copyLength = endOffset - startOffset + 1;
        StreamingOutput output = dest -> {
          try (OzoneInputStream ozoneInputStream = keyDetails.getContent()) {
            ozoneInputStream.seek(startOffset);
            IOUtils.copyLarge(ozoneInputStream, dest, 0,
                copyLength, new byte[bufferSize]);
          }
        };
        responseBuilder = Response
            .ok(output)
            .header(CONTENT_LENGTH, copyLength);

        String contentRangeVal = RANGE_HEADER_SUPPORTED_UNIT + " " +
            rangeHeader.getStartOffset() + "-" + rangeHeader.getEndOffset() +
            "/" + length;

        responseBuilder.header(CONTENT_RANGE_HEADER, contentRangeVal);
      }
      responseBuilder.header(ACCEPT_RANGE_HEADER,
          RANGE_HEADER_SUPPORTED_UNIT);

      // if multiple query parameters having same name,
      // Only the first parameters will be recognized
      // eg:
      // http://localhost:9878/bucket/key?response-expires=1&response-expires=2
      // only response-expires=1 is valid
      MultivaluedMap<String, String> queryParams = context
          .getUriInfo().getQueryParameters();

      for (Map.Entry<String, String> entry :
          overrideQueryParameter.entrySet()) {
        String headerValue = headers.getHeaderString(entry.getKey());

        /* "Overriding Response Header" by query parameter, See:
        https://docs.aws.amazon.com/de_de/AmazonS3/latest/API/API_GetObject.html
        */
        String queryValue = queryParams.getFirst(entry.getValue());
        if (queryValue != null) {
          headerValue = queryValue;
        }
        if (headerValue != null) {
          responseBuilder.header(entry.getKey(), headerValue);
        }
      }
      addLastModifiedDate(responseBuilder, keyDetails);
      getMetrics().incGetKeySuccess();
      return responseBuilder.build();
    } catch (OMException ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex)
      );
      if (uploadId != null) {
        getMetrics().incListPartsFailure();
      } else {
        getMetrics().incGetKeyFailure();
      }
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_KEY, keyPath, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditSuccess = false;
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex)
      );
      throw ex;
    } finally {
      if (auditSuccess) {
        AUDIT.logReadSuccess(
            buildAuditMessageForSuccess(s3GAction, getAuditParameters())
        );
      }
    }
  }

  static void addLastModifiedDate(
      ResponseBuilder responseBuilder, OzoneKey key) {

    ZonedDateTime lastModificationTime = key.getModificationTime()
        .atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));

    responseBuilder
        .header(LAST_MODIFIED,
            RFC1123Util.FORMAT.format(lastModificationTime));
  }

  /**
   * Rest endpoint to check existence of an object in a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath) throws IOException, OS3Exception {

    S3GAction s3GAction = S3GAction.HEAD_KEY;

    OzoneKey key;
    try {
      OzoneVolume volume = getVolume();
      key = getClientProtocol().headObject(volume.getName(),
          bucketName, keyPath);
      // TODO: return the specified range bytes of this object.
    } catch (OMException ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      getMetrics().incHeadKeyFailure();
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        // Just return 404 with no content
        return Response.status(Status.NOT_FOUND).build();
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      AUDIT.logReadFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      throw ex;
    }

    ResponseBuilder response = Response.ok().status(HttpStatus.SC_OK)
        .header("ETag", "" + key.getModificationTime())
        .header("Content-Length", key.getDataSize())
        .header("Content-Type", "binary/octet-stream");
    addLastModifiedDate(response, key);
    getMetrics().incHeadKeySuccess();
    AUDIT.logReadSuccess(buildAuditMessageForSuccess(s3GAction,
        getAuditParameters()));
    return response.build();
  }

  /**
   * Abort multipart upload request.
   * @param bucket
   * @param key
   * @param uploadId
   * @return Response
   * @throws IOException
   * @throws OS3Exception
   */
  private Response abortMultipartUpload(OzoneVolume volume, String bucket,
                                        String key, String uploadId)
      throws IOException, OS3Exception {
    try {
      getClientProtocol().abortMultipartUpload(volume.getName(), bucket,
          key, uploadId);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(S3ErrorTable.NO_SUCH_UPLOAD, uploadId, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucket, ex);
      }
      throw ex;
    }
    getMetrics().incAbortMultiPartUploadSuccess();
    return Response
        .status(Status.NO_CONTENT)
        .build();
  }


  /**
   * Delete a specific object from a bucket, if query param uploadId is
   * specified, this request is for abort multipart upload.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
   * https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
   * for more details.
   */
  @DELETE
  @SuppressWarnings("emptyblock")
  public Response delete(
      @PathParam("bucket") String bucketName,
      @PathParam("path") String keyPath,
      @QueryParam("uploadId") @DefaultValue("") String uploadId) throws
      IOException, OS3Exception {

    S3GAction s3GAction = S3GAction.DELETE_KEY;

    try {
      OzoneVolume volume = getVolume();
      if (uploadId != null && !uploadId.equals("")) {
        s3GAction = S3GAction.ABORT_MULTIPART_UPLOAD;
        return abortMultipartUpload(volume, bucketName, keyPath, uploadId);
      }
      getClientProtocol().deleteKey(volume.getName(), bucketName,
          keyPath, false);
    } catch (OMException ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      if (uploadId != null && !uploadId.equals("")) {
        getMetrics().incAbortMultiPartUploadFailure();
      } else {
        getMetrics().incDeleteKeyFailure();
      }
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        //NOT_FOUND is not a problem, AWS doesn't throw exception for missing
        // keys. Just return 204
      } else if (ex.getResult() == ResultCodes.DIRECTORY_NOT_EMPTY) {
        // With PREFIX metadata layout, a dir deletion without recursive flag
        // to true will throw DIRECTORY_NOT_EMPTY error for a non-empty dir.
        // NOT_FOUND is not a problem, AWS doesn't throw exception for missing
        // keys. Just return 204
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      if (uploadId != null && !uploadId.equals("")) {
        getMetrics().incAbortMultiPartUploadFailure();
      } else {
        getMetrics().incDeleteKeyFailure();
      }
      throw ex;
    }
    getMetrics().incDeleteKeySuccess();
    AUDIT.logWriteSuccess(buildAuditMessageForSuccess(s3GAction,
        getAuditParameters()));
    return Response
        .status(Status.NO_CONTENT)
        .build();
  }

  /**
   * Initialize MultiPartUpload request.
   * <p>
   * Note: the specific content type is set by the HeaderPreprocessor.
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  @Consumes(HeaderPreprocessor.MULTIPART_UPLOAD_MARKER)
  public Response initializeMultipartUpload(
      @PathParam("bucket") String bucket,
      @PathParam("path") String key
  )
      throws IOException, OS3Exception {
    S3GAction s3GAction = S3GAction.INIT_MULTIPART_UPLOAD;

    try {
      OzoneBucket ozoneBucket = getBucket(bucket);
      String storageType = headers.getHeaderString(STORAGE_CLASS_HEADER);

      ReplicationConfig replicationConfig =
          getReplicationConfig(ozoneBucket, storageType);

      OmMultipartInfo multipartInfo =
          ozoneBucket.initiateMultipartUpload(key, replicationConfig);

      MultipartUploadInitiateResponse multipartUploadInitiateResponse = new
          MultipartUploadInitiateResponse();

      multipartUploadInitiateResponse.setBucket(bucket);
      multipartUploadInitiateResponse.setKey(key);
      multipartUploadInitiateResponse.setUploadID(multipartInfo.getUploadID());

      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      getMetrics().incInitMultiPartUploadSuccess();
      return Response.status(Status.OK).entity(
          multipartUploadInitiateResponse).build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().incInitMultiPartUploadFailure();
      if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, key, ex);
      }
      throw ex;
    } catch (Exception ex) {
      AUDIT.logWriteFailure(
          buildAuditMessageForFailure(s3GAction, getAuditParameters(), ex));
      getMetrics().incInitMultiPartUploadFailure();
      throw ex;
    }
  }

  private ReplicationConfig getReplicationConfig(OzoneBucket ozoneBucket,
      String storageType) throws OS3Exception {
    if (StringUtils.isEmpty(storageType)) {
      storageType = S3StorageType.getDefault(ozoneConfiguration).toString();
    }

    ReplicationConfig clientConfiguredReplicationConfig = null;
    String replication = ozoneConfiguration.get(OZONE_REPLICATION);
    if (replication != null) {
      clientConfiguredReplicationConfig = ReplicationConfig.parse(
          ReplicationType.valueOf(ozoneConfiguration
              .get(OZONE_REPLICATION_TYPE, OZONE_REPLICATION_TYPE_DEFAULT)),
          replication, ozoneConfiguration);
    }
    return S3Utils.resolveS3ClientSideReplicationConfig(storageType,
        clientConfiguredReplicationConfig, ozoneBucket.getReplicationConfig());
  }

  /**
   * Complete a multipart upload.
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response completeMultipartUpload(@PathParam("bucket") String bucket,
      @PathParam("path") String key,
      @QueryParam("uploadId") @DefaultValue("") String uploadID,
      CompleteMultipartUploadRequest multipartUploadRequest)
      throws IOException, OS3Exception {
    S3GAction s3GAction = S3GAction.COMPLETE_MULTIPART_UPLOAD;
    OzoneVolume volume = getVolume();
    // Using LinkedHashMap to preserve ordering of parts list.
    Map<Integer, String> partsMap = new LinkedHashMap<>();
    List<CompleteMultipartUploadRequest.Part> partList =
        multipartUploadRequest.getPartList();

    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo;
    try {
      for (CompleteMultipartUploadRequest.Part part : partList) {
        partsMap.put(part.getPartNumber(), part.geteTag());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parts map {}", partsMap);
      }

      omMultipartUploadCompleteInfo = getClientProtocol()
          .completeMultipartUpload(volume.getName(), bucket, key, uploadID,
              partsMap);
      CompleteMultipartUploadResponse completeMultipartUploadResponse =
          new CompleteMultipartUploadResponse();
      completeMultipartUploadResponse.setBucket(bucket);
      completeMultipartUploadResponse.setKey(key);
      completeMultipartUploadResponse.setETag(omMultipartUploadCompleteInfo
          .getHash());
      // Location also setting as bucket name.
      completeMultipartUploadResponse.setLocation(bucket);
      AUDIT.logWriteSuccess(
          buildAuditMessageForSuccess(s3GAction, getAuditParameters()));
      getMetrics().incCompleteMultiPartUploadSuccess();
      return Response.status(Status.OK).entity(completeMultipartUploadResponse)
          .build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().incCompleteMultiPartUploadFailure();
      if (ex.getResult() == ResultCodes.INVALID_PART) {
        throw newError(S3ErrorTable.INVALID_PART, key, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_PART_ORDER) {
        throw newError(S3ErrorTable.INVALID_PART_ORDER, key, ex);
      } else if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (ex.getResult() == ResultCodes.ENTITY_TOO_SMALL) {
        throw newError(ENTITY_TOO_SMALL, key, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_REQUEST) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, key, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the CompleteMultipartUpload operation: You must " +
            "specify at least one part");
        throw os3Exception;
      } else if (ex.getResult() == ResultCodes.NOT_A_FILE) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, key, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the CompleteMultipartUpload operation: " +
            OZONE_OM_ENABLE_FILESYSTEM_PATHS + " is enabled Keys are " +
            "considered as Unix Paths. A directory already exists with a " +
            "given KeyName caused failure for MPU");
        throw os3Exception;
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucket, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }
  }

  private Response createMultipartKey(OzoneVolume volume, String bucket,
                                      String key, long length, int partNumber,
                                      String uploadID, InputStream body)
      throws IOException, OS3Exception {
    try {
      String copyHeader;
      OzoneOutputStream ozoneOutputStream = null;

      if ("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
          .equals(headers.getHeaderString("x-amz-content-sha256"))) {
        body = new SignedChunksInputStream(body);
      }

      try {
        ozoneOutputStream = getClientProtocol().createMultipartKey(
            volume.getName(), bucket, key, length, partNumber, uploadID);
        copyHeader = headers.getHeaderString(COPY_SOURCE_HEADER);
        if (copyHeader != null) {
          Pair<String, String> result = parseSourceHeader(copyHeader);

          String sourceBucket = result.getLeft();
          String sourceKey = result.getRight();

          OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
              volume.getName(), sourceBucket, sourceKey);
          Long sourceKeyModificationTime = sourceKeyDetails
              .getModificationTime().toEpochMilli();
          String copySourceIfModifiedSince =
              headers.getHeaderString(COPY_SOURCE_IF_MODIFIED_SINCE);
          String copySourceIfUnmodifiedSince =
              headers.getHeaderString(COPY_SOURCE_IF_UNMODIFIED_SINCE);
          if (!checkCopySourceModificationTime(sourceKeyModificationTime,
              copySourceIfModifiedSince, copySourceIfUnmodifiedSince)) {
            throw newError(PRECOND_FAILED, sourceBucket + "/" + sourceKey);
          }

          try (OzoneInputStream sourceObject = sourceKeyDetails.getContent()) {

            String range =
                headers.getHeaderString(COPY_SOURCE_HEADER_RANGE);
            if (range != null) {
              RangeHeader rangeHeader =
                  RangeHeaderParserUtil.parseRangeHeader(range, 0);
              final long skipped =
                  sourceObject.skip(rangeHeader.getStartOffset());
              if (skipped != rangeHeader.getStartOffset()) {
                throw new EOFException(
                    "Bytes to skip: "
                        + rangeHeader.getStartOffset() + " actual: " + skipped);
              }
              IOUtils.copyLarge(sourceObject, ozoneOutputStream, 0,
                  rangeHeader.getEndOffset() - rangeHeader.getStartOffset()
                      + 1);
            } else {
              IOUtils.copy(sourceObject, ozoneOutputStream);
            }
          }
        } else {
          IOUtils.copy(body, ozoneOutputStream);
        }
      } finally {
        if (ozoneOutputStream != null) {
          ozoneOutputStream.close();
        }
      }

      assert ozoneOutputStream != null;
      OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
          ozoneOutputStream.getCommitUploadPartInfo();
      String eTag = omMultipartCommitUploadPartInfo.getPartName();

      getMetrics().incCreateMultipartKeySuccess();
      if (copyHeader != null) {
        return Response.ok(new CopyPartResult(eTag)).build();
      } else {
        return Response.ok().header("ETag",
            eTag).build();
      }

    } catch (OMException ex) {
      getMetrics().incCreateMultipartKeyFailure();
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucket + "/" + key, ex);
      }
      throw ex;
    }
  }

  /**
   * Returns response for the listParts request.
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * @param bucket
   * @param key
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return
   * @throws IOException
   * @throws OS3Exception
   */
  private Response listParts(String bucket, String key, String uploadID,
      int partNumberMarker, int maxParts) throws IOException, OS3Exception {
    ListPartsResponse listPartsResponse = new ListPartsResponse();
    try {
      OzoneBucket ozoneBucket = getBucket(bucket);
      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          ozoneBucket.listParts(key, uploadID, partNumberMarker, maxParts);
      listPartsResponse.setBucket(bucket);
      listPartsResponse.setKey(key);
      listPartsResponse.setUploadID(uploadID);
      listPartsResponse.setMaxParts(maxParts);
      listPartsResponse.setPartNumberMarker(partNumberMarker);
      listPartsResponse.setTruncated(false);

      listPartsResponse.setStorageClass(S3StorageType.fromReplicationConfig(
          ozoneMultipartUploadPartListParts.getReplicationConfig()).toString());

      if (ozoneMultipartUploadPartListParts.isTruncated()) {
        listPartsResponse.setTruncated(
            ozoneMultipartUploadPartListParts.isTruncated());
        listPartsResponse.setNextPartNumberMarker(
            ozoneMultipartUploadPartListParts.getNextPartNumberMarker());
      }

      ozoneMultipartUploadPartListParts.getPartInfoList().forEach(partInfo -> {
        ListPartsResponse.Part part = new ListPartsResponse.Part();
        part.setPartNumber(partInfo.getPartNumber());
        part.setETag(partInfo.getPartName());
        part.setSize(partInfo.getSize());
        part.setLastModified(Instant.ofEpochMilli(
            partInfo.getModificationTime()));
        listPartsResponse.addPart(part);
      });

    } catch (OMException ex) {
      getMetrics().incListPartsFailure();
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            bucket + "/" + key + "/" + uploadID, ex);
      }
      throw ex;
    }
    getMetrics().incListPartsSuccess();
    return Response.status(Status.OK).entity(listPartsResponse).build();
  }

  @VisibleForTesting
  public void setHeaders(HttpHeaders headers) {
    this.headers = headers;
  }

  @VisibleForTesting
  public void setContext(ContainerRequestContext context) {
    this.context = context;
  }

  void copy(OzoneVolume volume, InputStream src, long srcKeyLen,
      String destKey, String destBucket,
      ReplicationConfig replication) throws IOException {
    try (OzoneOutputStream dest = getClientProtocol().createKey(
        volume.getName(), destBucket, destKey, srcKeyLen,
        replication, new HashMap<>())) {
      IOUtils.copy(src, dest);
    }
  }

  private CopyObjectResponse copyObject(OzoneVolume volume,
                                        String copyHeader,
                                        String destBucket,
                                        String destkey,
                                        ReplicationConfig replicationConfig,
                                        boolean storageTypeDefault)
      throws OS3Exception, IOException {

    Pair<String, String> result = parseSourceHeader(copyHeader);

    String sourceBucket = result.getLeft();
    String sourceKey = result.getRight();
    try {
      // Checking whether we trying to copying to it self.

      if (sourceBucket.equals(destBucket) && sourceKey
          .equals(destkey)) {
        // When copying to same storage type when storage type is provided,
        // we should not throw exception, as aws cli checks if any of the
        // options like storage type are provided or not when source and
        // dest are given same
        if (storageTypeDefault) {
          OS3Exception ex = newError(S3ErrorTable.INVALID_REQUEST, copyHeader);
          ex.setErrorMessage("This copy request is illegal because it is " +
              "trying to copy an object to it self itself without changing " +
              "the object's metadata, storage class, website redirect " +
              "location or encryption attributes.");
          throw ex;
        } else {
          // TODO: Actually here we should change storage type, as ozone
          // still does not support this just returning dummy response
          // for now
          CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
          copyObjectResponse.setETag(OzoneUtils.getRequestID());
          copyObjectResponse.setLastModified(Instant.ofEpochMilli(
              Time.now()));
          return copyObjectResponse;
        }
      }

      OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
          volume.getName(), sourceBucket, sourceKey);
      long sourceKeyLen = sourceKeyDetails.getDataSize();

      try (OzoneInputStream src = getClientProtocol().getKey(volume.getName(),
          sourceBucket, sourceKey)) {
        copy(volume, src, sourceKeyLen, destkey, destBucket, replicationConfig);
      }

      final OzoneKeyDetails destKeyDetails = getClientProtocol().getKeyDetails(
          volume.getName(), destBucket, destkey);

      getMetrics().incCopyObjectSuccess();
      CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
      copyObjectResponse.setETag(OzoneUtils.getRequestID());
      copyObjectResponse.setLastModified(destKeyDetails.getModificationTime());
      return copyObjectResponse;
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_KEY, sourceKey, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, sourceBucket, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            destBucket + "/" + destkey, ex);
      }
      throw ex;
    }
  }

  /**
   * Parse the key and bucket name from copy header.
   */
  @VisibleForTesting
  public static Pair<String, String> parseSourceHeader(String copyHeader)
      throws OS3Exception {
    String header = copyHeader;
    if (header.startsWith("/")) {
      header = copyHeader.substring(1);
    }
    int pos = header.indexOf('/');
    if (pos == -1) {
      OS3Exception ex = newError(INVALID_ARGUMENT, header);
      ex.setErrorMessage("Copy Source must mention the source bucket and " +
          "key: sourcebucket/sourcekey");
      throw ex;
    }

    try {
      String bucket = header.substring(0, pos);
      String key = urlDecode(header.substring(pos + 1));
      return Pair.of(bucket, key);
    } catch (UnsupportedEncodingException e) {
      OS3Exception ex = newError(INVALID_ARGUMENT, header, e);
      ex.setErrorMessage("Copy Source header could not be url-decoded");
      throw ex;
    }
  }

  private static int parsePartNumberMarker(String partNumberMarker) {
    int partMarker = 0;
    if (partNumberMarker != null) {
      partMarker = Integer.parseInt(partNumberMarker);
    }
    return partMarker;
  }

  // Parses date string and return long representation. Returns an
  // empty if DateStr is null or invalid. Dates in the future are
  // considered invalid.
  private static OptionalLong parseAndValidateDate(String ozoneDateStr) {
    long ozoneDateInMs;
    if (ozoneDateStr == null) {
      return OptionalLong.empty();
    }
    try {
      ozoneDateInMs = OzoneUtils.formatDate(ozoneDateStr);
    } catch (ParseException e) {
      // if time not parseable, then return empty()
      return OptionalLong.empty();
    }

    long currentDate = System.currentTimeMillis();
    if (ozoneDateInMs <= currentDate) {
      return OptionalLong.of(ozoneDateInMs);
    } else {
      // dates in the future are invalid, so return empty()
      return OptionalLong.empty();
    }
  }

  static boolean checkCopySourceModificationTime(Long lastModificationTime,
      String copySourceIfModifiedSinceStr,
      String copySourceIfUnmodifiedSinceStr) {
    long copySourceIfModifiedSince = Long.MIN_VALUE;
    long copySourceIfUnmodifiedSince = Long.MAX_VALUE;

    OptionalLong modifiedDate =
        parseAndValidateDate(copySourceIfModifiedSinceStr);
    if (modifiedDate.isPresent()) {
      copySourceIfModifiedSince = modifiedDate.getAsLong();
    }

    OptionalLong unmodifiedDate =
        parseAndValidateDate(copySourceIfUnmodifiedSinceStr);
    if (unmodifiedDate.isPresent()) {
      copySourceIfUnmodifiedSince = unmodifiedDate.getAsLong();
    }
    return (copySourceIfModifiedSince <= lastModificationTime) &&
        (lastModificationTime <= copySourceIfUnmodifiedSince);
  }

  @VisibleForTesting
  public void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }
}
