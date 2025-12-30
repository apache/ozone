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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_CLIENT_BUFFER_SIZE_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ENTITY_TOO_SMALL;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCEPT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CONTENT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CopyDirective;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MP_PARTS_COUNT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER_SUPPORTED_UNIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CONFIG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_COUNT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.hasMultiChunksPayload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.hasUnsignedPayload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlDecode;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateMultiChunksUpload;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateSignatureHeader;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.PostConstruct;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.DatatypeConverter;
import net.jcip.annotations.Immutable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream;
import org.apache.hadoop.ozone.s3.UnsignedChunksInputStream;
import org.apache.hadoop.ozone.s3.endpoint.S3Tagging.Tag;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.RangeHeaderParserUtil;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key level rest endpoints.
 */
@Path("/{bucket}/{path:.+}")
public class ObjectEndpoint extends EndpointBase {

  private static final String BUCKET = "bucket";
  private static final String PATH = "path";

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpoint.class);

  private static final ThreadLocal<MessageDigest> E_TAG_PROVIDER;
  private static final ThreadLocal<MessageDigest> SHA_256_PROVIDER;

  static {
    E_TAG_PROVIDER = ThreadLocal.withInitial(() -> {
      try {
        return MessageDigest.getInstance(OzoneConsts.MD5_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    });

    SHA_256_PROVIDER = ThreadLocal.withInitial(() -> {
      try {
        return MessageDigest.getInstance(OzoneConsts.FILE_HASH);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /*FOR the feature Overriding Response Header
  https://docs.aws.amazon.com/de_de/AmazonS3/latest/API/API_GetObject.html */
  private final Map<String, String> overrideQueryParameter;
  private int bufferSize;
  private int chunkSize;
  private boolean datastreamEnabled;
  private long datastreamMinLength;

  public ObjectEndpoint() {
    overrideQueryParameter = ImmutableMap.<String, String>builder()
        .put(HttpHeaders.CONTENT_TYPE, "response-content-type")
        .put(HttpHeaders.CONTENT_LANGUAGE, "response-content-language")
        .put(HttpHeaders.EXPIRES, "response-expires")
        .put(HttpHeaders.CACHE_CONTROL, "response-cache-control")
        .put(HttpHeaders.CONTENT_DISPOSITION, "response-content-disposition")
        .put(HttpHeaders.CONTENT_ENCODING, "response-content-encoding")
        .build();
  }

  @Override
  @PostConstruct
  public void init() {
    bufferSize = (int) getOzoneConfiguration().getStorageSize(
        OZONE_S3G_CLIENT_BUFFER_SIZE_KEY,
        OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT, StorageUnit.BYTES);
    chunkSize = (int) getOzoneConfiguration().getStorageSize(
        OZONE_SCM_CHUNK_SIZE_KEY,
        OZONE_SCM_CHUNK_SIZE_DEFAULT,
        StorageUnit.BYTES);
    datastreamEnabled = getOzoneConfiguration().getBoolean(
        HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED_DEFAULT);
    datastreamMinLength = (long) getOzoneConfiguration().getStorageSize(
        OZONE_FS_DATASTREAM_AUTO_THRESHOLD,
        OZONE_FS_DATASTREAM_AUTO_THRESHOLD_DEFAULT, StorageUnit.BYTES);
  }

  /**
   * Rest endpoint to upload object to a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html for
   * more details.
   */
  @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:ParameterNumber"})
  @PUT
  public Response put(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath,
      @HeaderParam(HttpHeaders.CONTENT_LENGTH) long length,
      @QueryParam(QueryParams.PART_NUMBER)  int partNumber,
      @QueryParam(QueryParams.UPLOAD_ID) @DefaultValue("") String uploadID,
      @QueryParam(QueryParams.TAGGING) String taggingMarker,
      @QueryParam(QueryParams.ACL) String aclMarker,
      final InputStream body) throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.CREATE_KEY;
    boolean auditSuccess = true;
    PerformanceStringBuilder perf = new PerformanceStringBuilder();

    String copyHeader = null, storageType = null, storageConfig = null;
    MultiDigestInputStream multiDigestInputStream = null;
    try {
      if (aclMarker != null) {
        s3GAction = S3GAction.PUT_OBJECT_ACL;
        throw newError(NOT_IMPLEMENTED, keyPath);
      }
      OzoneVolume volume = getVolume();
      OzoneBucket bucket = volume.getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      if (taggingMarker != null) {
        s3GAction = S3GAction.PUT_OBJECT_TAGGING;
        return putObjectTagging(bucket, keyPath, body);
      }

      if (uploadID != null && !uploadID.equals("")) {
        if (getHeaders().getHeaderString(COPY_SOURCE_HEADER) == null) {
          s3GAction = S3GAction.CREATE_MULTIPART_KEY;
        } else {
          s3GAction = S3GAction.CREATE_MULTIPART_KEY_BY_COPY;
        }
        // If uploadID is specified, it is a request for upload part
        return createMultipartKey(volume, bucket, keyPath, length,
            partNumber, uploadID, body, perf);
      }

      copyHeader = getHeaders().getHeaderString(COPY_SOURCE_HEADER);
      storageType = getHeaders().getHeaderString(STORAGE_CLASS_HEADER);
      storageConfig = getHeaders().getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + STORAGE_CONFIG_HEADER);
      boolean storageTypeDefault = StringUtils.isEmpty(storageType);

      // Normal put object
      ReplicationConfig replicationConfig =
          getReplicationConfig(bucket, storageType, storageConfig);

      boolean enableEC = false;
      if ((replicationConfig != null &&
          replicationConfig.getReplicationType() == EC) ||
          bucket.getReplicationConfig() instanceof ECReplicationConfig) {
        enableEC = true;
      }

      if (copyHeader != null) {
        //Copy object, as copy source available.
        s3GAction = S3GAction.COPY_OBJECT;
        CopyObjectResponse copyObjectResponse = copyObject(volume,
            copyHeader, bucketName, keyPath, replicationConfig,
            storageTypeDefault, perf);
        return Response.status(Status.OK).entity(copyObjectResponse).header(
            "Connection", "close").build();
      }

      boolean canCreateDirectory = getOzoneConfiguration()
          .getBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED,
              OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT) &&
          bucket.getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED;

      String amzDecodedLength =
          getHeaders().getHeaderString(S3Consts.DECODED_CONTENT_LENGTH_HEADER);
      boolean hasAmzDecodedLengthZero = amzDecodedLength != null &&
          Long.parseLong(amzDecodedLength) == 0;
      if (canCreateDirectory &&
          (length == 0 || hasAmzDecodedLengthZero) &&
          StringUtils.endsWith(keyPath, "/")
      ) {
        s3GAction = S3GAction.CREATE_DIRECTORY;
        getClientProtocol()
            .createDirectory(volume.getName(), bucketName, keyPath);
        long metadataLatencyNs =
            getMetrics().updatePutKeyMetadataStats(startNanos);
        perf.appendMetaLatencyNanos(metadataLatencyNs);
        return Response.ok().status(HttpStatus.SC_OK).build();
      }

      // Normal put object
      S3ChunkInputStreamInfo chunkInputStreamInfo = getS3ChunkInputStreamInfo(body,
          length, amzDecodedLength, keyPath);
      multiDigestInputStream = chunkInputStreamInfo.getMultiDigestInputStream();
      length = chunkInputStreamInfo.getEffectiveLength();

      Map<String, String> customMetadata =
          getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());
      Map<String, String> tags = getTaggingFromHeaders(getHeaders());

      long putLength;
      String eTag = null;
      if (datastreamEnabled && !enableEC && length > datastreamMinLength) {
        perf.appendStreamMode();
        Pair<String, Long> keyWriteResult = ObjectEndpointStreaming
            .put(bucket, keyPath, length, replicationConfig, chunkSize,
                customMetadata, tags, multiDigestInputStream, getHeaders(), signatureInfo.isSignPayload(), perf);
        eTag = keyWriteResult.getKey();
        putLength = keyWriteResult.getValue();
      } else {
        final String amzContentSha256Header =
            validateSignatureHeader(getHeaders(), keyPath, signatureInfo.isSignPayload());
        try (OzoneOutputStream output = getClientProtocol().createKey(
            volume.getName(), bucketName, keyPath, length, replicationConfig,
            customMetadata, tags)) {
          long metadataLatencyNs =
              getMetrics().updatePutKeyMetadataStats(startNanos);
          perf.appendMetaLatencyNanos(metadataLatencyNs);
          putLength = IOUtils.copyLarge(multiDigestInputStream, output, 0, length,
              new byte[getIOBufferSize(length)]);
          eTag = DatatypeConverter.printHexBinary(
                  multiDigestInputStream.getMessageDigest(OzoneConsts.MD5_HASH).digest())
              .toLowerCase();
          output.getMetadata().put(OzoneConsts.ETAG, eTag);

          // If sha256Digest exists, this request must validate x-amz-content-sha256
          MessageDigest sha256Digest = multiDigestInputStream.getMessageDigest(OzoneConsts.FILE_HASH);
          if (sha256Digest != null) {
            final String actualSha256 = DatatypeConverter.printHexBinary(
                sha256Digest.digest()).toLowerCase();
            Runnable preCommit = () -> {
              if (!amzContentSha256Header.equals(actualSha256)) {
                throw S3ErrorTable.newError(S3ErrorTable.X_AMZ_CONTENT_SHA256_MISMATCH, keyPath);
              }
            };
            output.getKeyOutputStream().setPreCommits(Collections.singletonList(preCommit));
          }
        }
      }
      getMetrics().incPutKeySuccessLength(putLength);
      perf.appendSizeBytes(putLength);
      return Response.ok()
          .header(HttpHeaders.ETAG, wrapInQuotes(eTag))
          .status(HttpStatus.SC_OK)
          .build();
    } catch (OMException ex) {
      auditSuccess = false;
      auditWriteFailure(s3GAction, ex);
      if (taggingMarker != null) {
        getMetrics().updatePutObjectTaggingFailureStats(startNanos);
      } else if (copyHeader != null) {
        getMetrics().updateCopyObjectFailureStats(startNanos);
      } else {
        getMetrics().updateCreateKeyFailureStats(startNanos);
      }
      if (ex.getResult() == ResultCodes.NOT_A_FILE) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, keyPath, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the PutObject/MPU PartUpload operation: " +
            OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
            " considered as Unix Paths. Path has Violated FS Semantics " +
            "which caused put operation to fail.");
        throw os3Exception;
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.QUOTA_EXCEEDED) {
        throw newError(S3ErrorTable.QUOTA_EXCEEDED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.FILE_ALREADY_EXISTS) {
        throw newError(S3ErrorTable.NO_OVERWRITE, keyPath, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditSuccess = false;
      auditWriteFailure(s3GAction, ex);
      if (aclMarker != null) {
        getMetrics().updatePutObjectAclFailureStats(startNanos);
      } else if (taggingMarker != null) {
        getMetrics().updatePutObjectTaggingFailureStats(startNanos);
      } else if (copyHeader != null) {
        getMetrics().updateCopyObjectFailureStats(startNanos);
      } else {
        getMetrics().updateCreateKeyFailureStats(startNanos);
      }
      throw ex;
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (multiDigestInputStream != null) {
        multiDigestInputStream.resetDigests();
      }
      if (auditSuccess) {
        long opLatencyNs = getMetrics().updateCreateKeySuccessStats(startNanos);
        perf.appendOpLatencyNanos(opLatencyNs);
        auditWriteSuccess(s3GAction, perf);
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
  @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:ParameterNumber"})
  @GET
  public Response get(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath,
      @QueryParam(QueryParams.PART_NUMBER) int partNumber,
      @QueryParam(QueryParams.UPLOAD_ID) String uploadId,
      @QueryParam(QueryParams.MAX_PARTS) @DefaultValue("1000") int maxParts,
      @QueryParam(QueryParams.PART_NUMBER_MARKER) String partNumberMarker,
      @QueryParam(QueryParams.TAGGING) String taggingMarker)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.GET_KEY;
    PerformanceStringBuilder perf = new PerformanceStringBuilder();
    try {
      OzoneBucket bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      if (taggingMarker != null) {
        s3GAction = S3GAction.GET_OBJECT_TAGGING;
        return getObjectTagging(bucket, keyPath);
      }

      if (uploadId != null) {
        // When we have uploadId, this is the request for list Parts.
        s3GAction = S3GAction.LIST_PARTS;
        int partMarker = parsePartNumberMarker(partNumberMarker);
        Response response = listParts(bucket, keyPath, uploadId,
            partMarker, maxParts, perf);
        auditReadSuccess(s3GAction, perf);
        return response;
      }

      OzoneKeyDetails keyDetails = (partNumber != 0) ?
          getClientProtocol().getS3KeyDetails(bucketName, keyPath, partNumber) :
          getClientProtocol().getS3KeyDetails(bucketName, keyPath);

      isFile(keyPath, keyDetails);

      long length = keyDetails.getDataSize();

      LOG.debug("Data length of the key {} is {}", keyPath, length);

      String rangeHeaderVal = getHeaders().getHeaderString(RANGE_HEADER);
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
            long readLength = IOUtils.copy(key, dest, getIOBufferSize(keyDetails.getDataSize()));
            getMetrics().incGetKeySuccessLength(readLength);
            perf.appendSizeBytes(readLength);
          }
          long opLatencyNs =  getMetrics().updateGetKeySuccessStats(startNanos);
          perf.appendOpLatencyNanos(opLatencyNs);
          auditReadSuccess(S3GAction.GET_KEY, perf);
        };
        responseBuilder = Response
            .ok(output)
            .header(HttpHeaders.CONTENT_LENGTH, keyDetails.getDataSize());

      } else {

        long startOffset = rangeHeader.getStartOffset();
        long endOffset = rangeHeader.getEndOffset();
        // eg. if range header is given as bytes=0-0, then we should return 1
        // byte from start offset
        long copyLength = endOffset - startOffset + 1;
        StreamingOutput output = dest -> {
          try (OzoneInputStream ozoneInputStream = keyDetails.getContent()) {
            ozoneInputStream.seek(startOffset);
            long readLength = IOUtils.copyLarge(ozoneInputStream, dest, 0,
                copyLength, new byte[getIOBufferSize(copyLength)]);
            getMetrics().incGetKeySuccessLength(readLength);
            perf.appendSizeBytes(readLength);
          }
          long opLatencyNs = getMetrics().updateGetKeySuccessStats(startNanos);
          perf.appendOpLatencyNanos(opLatencyNs);
          auditReadSuccess(S3GAction.GET_KEY, perf);
        };
        responseBuilder = Response
            .status(Status.PARTIAL_CONTENT)
            .entity(output)
            .header(HttpHeaders.CONTENT_LENGTH, copyLength);

        String contentRangeVal = RANGE_HEADER_SUPPORTED_UNIT + " " +
            rangeHeader.getStartOffset() + "-" + rangeHeader.getEndOffset() +
            "/" + length;

        responseBuilder.header(CONTENT_RANGE_HEADER, contentRangeVal);
      }
      responseBuilder
          .header(ACCEPT_RANGE_HEADER, RANGE_HEADER_SUPPORTED_UNIT);

      String eTag = keyDetails.getMetadata().get(OzoneConsts.ETAG);
      if (eTag != null) {
        responseBuilder.header(HttpHeaders.ETAG, wrapInQuotes(eTag));
        String partsCount = extractPartsCount(eTag);
        if (partsCount != null) {
          responseBuilder.header(MP_PARTS_COUNT, partsCount);
        }
      }

      // if multiple query parameters having same name,
      // Only the first parameters will be recognized
      // eg:
      // http://localhost:9878/bucket/key?response-expires=1&response-expires=2
      // only response-expires=1 is valid
      MultivaluedMap<String, String> queryParams = getContext()
          .getUriInfo().getQueryParameters();

      for (Map.Entry<String, String> entry :
          overrideQueryParameter.entrySet()) {
        String headerValue = getHeaders().getHeaderString(entry.getKey());

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
      addTagCountIfAny(responseBuilder, keyDetails);
      long metadataLatencyNs =
          getMetrics().updateGetKeyMetadataStats(startNanos);
      perf.appendMetaLatencyNanos(metadataLatencyNs);
      return responseBuilder.build();
    } catch (OMException ex) {
      auditReadFailure(s3GAction, ex);
      if (taggingMarker != null) {
        getMetrics().updateGetObjectTaggingFailureStats(startNanos);
      } else if (uploadId != null) {
        getMetrics().updateListPartsFailureStats(startNanos);
      } else {
        getMetrics().updateGetKeyFailureStats(startNanos);
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
      auditReadFailure(s3GAction, ex);
      throw ex;
    }
  }

  static void addLastModifiedDate(
      ResponseBuilder responseBuilder, OzoneKey key) {

    ZonedDateTime lastModificationTime = key.getModificationTime()
        .atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));

    responseBuilder
        .header(HttpHeaders.LAST_MODIFIED,
            RFC1123Util.FORMAT.format(lastModificationTime));
  }

  static void addTagCountIfAny(
      ResponseBuilder responseBuilder, OzoneKey key) {
    // See x-amz-tagging-count in https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    // The number of tags, IF ANY, on the object, when you have the relevant
    // permission to read object tags
    if (!key.getTags().isEmpty()) {
      responseBuilder
          .header(TAG_COUNT_HEADER, key.getTags().size());
    }
  }

  /**
   * Rest endpoint to check existence of an object in a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath) throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.HEAD_KEY;

    OzoneKey key;
    try {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }
      key = getClientProtocol().headS3Object(bucketName, keyPath);

      isFile(keyPath, key);
      // TODO: return the specified range bytes of this object.
    } catch (OMException ex) {
      auditReadFailure(s3GAction, ex);
      getMetrics().updateHeadKeyFailureStats(startNanos);
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
      auditReadFailure(s3GAction, ex);
      throw ex;
    }

    S3StorageType s3StorageType = key.getReplicationConfig() == null ?
        S3StorageType.STANDARD :
        S3StorageType.fromReplicationConfig(key.getReplicationConfig());

    ResponseBuilder response = Response.ok().status(HttpStatus.SC_OK)
        .header(HttpHeaders.CONTENT_LENGTH, key.getDataSize())
        .header(HttpHeaders.CONTENT_TYPE, "binary/octet-stream")
        .header(STORAGE_CLASS_HEADER, s3StorageType.toString());

    String eTag = key.getMetadata().get(OzoneConsts.ETAG);
    if (eTag != null) {
      // Should not return ETag header if the ETag is not set
      // doing so will result in "null" string being returned instead
      // which breaks some AWS SDK implementation
      response.header(HttpHeaders.ETAG, wrapInQuotes(eTag));
      String partsCount = extractPartsCount(eTag);
      if (partsCount != null) {
        response.header(MP_PARTS_COUNT, partsCount);
      }
    }

    addLastModifiedDate(response, key);
    addCustomMetadataHeaders(response, key);
    getMetrics().updateHeadKeySuccessStats(startNanos);
    auditReadSuccess(s3GAction);
    return response.build();
  }

  private void isFile(String keyPath, OzoneKey key) throws OMException {
    /*
      Necessary for directories in buckets with FSO layout.
      Intended for apps which use Hadoop S3A.
      Example of such app is Trino (through Hive connector).
     */
    boolean isFsoDirCreationEnabled = getOzoneConfiguration()
        .getBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED,
            OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT);
    if (isFsoDirCreationEnabled &&
        !key.isFile() &&
        !keyPath.endsWith("/")) {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
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
    long startNanos = Time.monotonicNowNanos();
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
    getMetrics().updateAbortMultipartUploadSuccessStats(startNanos);
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
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath,
      @QueryParam(QueryParams.UPLOAD_ID) @DefaultValue("") String uploadId,
      @QueryParam(QueryParams.TAGGING) String taggingMarker) throws
      IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.DELETE_KEY;

    try {
      OzoneVolume volume = getVolume();
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = volume.getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }
      if (taggingMarker != null) {
        s3GAction = S3GAction.DELETE_OBJECT_TAGGING;
        return deleteObjectTagging(volume, bucketName, keyPath);
      }

      if (uploadId != null && !uploadId.equals("")) {
        s3GAction = S3GAction.ABORT_MULTIPART_UPLOAD;
        return abortMultipartUpload(volume, bucketName, keyPath, uploadId);
      }
      getClientProtocol().deleteKey(volume.getName(), bucketName,
          keyPath, false);
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      if (uploadId != null && !uploadId.equals("")) {
        getMetrics().updateAbortMultipartUploadFailureStats(startNanos);
      } else {
        getMetrics().updateDeleteKeyFailureStats(startNanos);
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
      } else if (ex.getResult() == ResultCodes.NOT_SUPPORTED_OPERATION) {
        // When deleteObjectTagging operation is applied on FSO directory
        throw S3ErrorTable.newError(S3ErrorTable.NOT_IMPLEMENTED, keyPath);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      if (taggingMarker != null) {
        getMetrics().updateDeleteObjectTaggingFailureStats(startNanos);
      } else if (uploadId != null && !uploadId.equals("")) {
        getMetrics().updateAbortMultipartUploadFailureStats(startNanos);
      } else {
        getMetrics().updateDeleteKeyFailureStats(startNanos);
      }
      throw ex;
    }
    getMetrics().updateDeleteKeySuccessStats(startNanos);
    auditWriteSuccess(s3GAction);
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
      @PathParam(BUCKET) String bucket,
      @PathParam(PATH) String key
  )
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.INIT_MULTIPART_UPLOAD;

    try {
      OzoneBucket ozoneBucket = getBucket(bucket);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucket, ozoneBucket.getOwner());
      String storageType = getHeaders().getHeaderString(STORAGE_CLASS_HEADER);
      String storageConfig = getHeaders().getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + STORAGE_CONFIG_HEADER);

      Map<String, String> customMetadata =
          getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());

      Map<String, String> tags = getTaggingFromHeaders(getHeaders());

      ReplicationConfig replicationConfig =
          getReplicationConfig(ozoneBucket, storageType, storageConfig);

      OmMultipartInfo multipartInfo =
          ozoneBucket.initiateMultipartUpload(key, replicationConfig, customMetadata, tags);

      MultipartUploadInitiateResponse multipartUploadInitiateResponse = new
          MultipartUploadInitiateResponse();

      multipartUploadInitiateResponse.setBucket(bucket);
      multipartUploadInitiateResponse.setKey(key);
      multipartUploadInitiateResponse.setUploadID(multipartInfo.getUploadID());

      auditWriteSuccess(s3GAction);
      getMetrics().updateInitMultipartUploadSuccessStats(startNanos);
      return Response.status(Status.OK).entity(
          multipartUploadInitiateResponse).build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateInitMultipartUploadFailureStats(startNanos);
      if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, key, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateInitMultipartUploadFailureStats(startNanos);
      throw ex;
    }
  }

  private ReplicationConfig getReplicationConfig(OzoneBucket ozoneBucket,
      String storageType, String storageConfig) throws OS3Exception {

    ReplicationConfig clientConfiguredReplicationConfig =
        OzoneClientUtils.getClientConfiguredReplicationConfig(getOzoneConfiguration());

    return S3Utils.resolveS3ClientSideReplicationConfig(storageType, storageConfig,
        clientConfiguredReplicationConfig, ozoneBucket.getReplicationConfig());
  }

  /**
   * Complete a multipart upload.
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response completeMultipartUpload(
      @PathParam(BUCKET) String bucket,
      @PathParam(PATH) String key,
      @QueryParam(QueryParams.UPLOAD_ID) @DefaultValue("") String uploadID,
      CompleteMultipartUploadRequest multipartUploadRequest)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.COMPLETE_MULTIPART_UPLOAD;
    OzoneVolume volume = getVolume();
    // Using LinkedHashMap to preserve ordering of parts list.
    Map<Integer, String> partsMap = new LinkedHashMap<>();
    List<CompleteMultipartUploadRequest.Part> partList =
        multipartUploadRequest.getPartList();

    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo;
    try {
      OzoneBucket ozoneBucket = volume.getBucket(bucket);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucket, ozoneBucket.getOwner());

      for (CompleteMultipartUploadRequest.Part part : partList) {
        partsMap.put(part.getPartNumber(), stripQuotes(part.getETag()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parts map {}", partsMap);
      }

      omMultipartUploadCompleteInfo = ozoneBucket.completeMultipartUpload(key, uploadID, partsMap);
      CompleteMultipartUploadResponse completeMultipartUploadResponse =
          new CompleteMultipartUploadResponse();
      completeMultipartUploadResponse.setBucket(bucket);
      completeMultipartUploadResponse.setKey(key);
      completeMultipartUploadResponse.setETag(
          wrapInQuotes(omMultipartUploadCompleteInfo.getHash()));
      // Location also setting as bucket name.
      completeMultipartUploadResponse.setLocation(bucket);
      auditWriteSuccess(s3GAction);
      getMetrics().updateCompleteMultipartUploadSuccessStats(startNanos);
      return Response.status(Status.OK).entity(completeMultipartUploadResponse)
          .build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateCompleteMultipartUploadFailureStats(startNanos);
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
            OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are " +
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

  @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:ParameterNumber"})
  private Response createMultipartKey(OzoneVolume volume, OzoneBucket ozoneBucket,
      String key, long length, int partNumber, String uploadID,
      final InputStream body, PerformanceStringBuilder perf)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    String copyHeader = null;
    MultiDigestInputStream multiDigestInputStream = null;
    final String bucketName = ozoneBucket.getName();
    try {
      String amzDecodedLength = getHeaders().getHeaderString(DECODED_CONTENT_LENGTH_HEADER);
      S3ChunkInputStreamInfo chunkInputStreamInfo = getS3ChunkInputStreamInfo(
          body, length, amzDecodedLength, key);
      multiDigestInputStream = chunkInputStreamInfo.getMultiDigestInputStream();
      length = chunkInputStreamInfo.getEffectiveLength();

      copyHeader = getHeaders().getHeaderString(COPY_SOURCE_HEADER);
      String storageType = getHeaders().getHeaderString(STORAGE_CLASS_HEADER);
      String storageConfig = getHeaders().getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + STORAGE_CONFIG_HEADER);
      ReplicationConfig replicationConfig =
          getReplicationConfig(ozoneBucket, storageType, storageConfig);

      boolean enableEC = false;
      if ((replicationConfig != null &&
          replicationConfig.getReplicationType()  == EC) ||
          ozoneBucket.getReplicationConfig() instanceof ECReplicationConfig) {
        enableEC = true;
      }

      if (datastreamEnabled && !enableEC && copyHeader == null) {
        perf.appendStreamMode();
        return ObjectEndpointStreaming
            .createMultipartKey(ozoneBucket, key, length, partNumber,
                uploadID, chunkSize, multiDigestInputStream, perf);
      }
      // OmMultipartCommitUploadPartInfo can only be gotten after the
      // OzoneOutputStream is closed, so we need to save the OzoneOutputStream
      final OzoneOutputStream outputStream;
      long metadataLatencyNs;
      if (copyHeader != null) {
        Pair<String, String> result = parseSourceHeader(copyHeader);
        String sourceBucket = result.getLeft();
        String sourceKey = result.getRight();
        if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
          String sourceBucketOwner = volume.getBucket(sourceBucket).getOwner();
          S3Owner.verifyBucketOwnerConditionOnCopyOperation(getHeaders(), sourceBucket, sourceBucketOwner, bucketName,
              ozoneBucket.getOwner());
        }

        OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
            volume.getName(), sourceBucket, sourceKey);
        String range =
            getHeaders().getHeaderString(COPY_SOURCE_HEADER_RANGE);
        RangeHeader rangeHeader = null;
        if (range != null) {
          rangeHeader = RangeHeaderParserUtil.parseRangeHeader(range, 0);
          // When copy Range, the size of the target key is the
          // length specified by COPY_SOURCE_HEADER_RANGE.
          length = rangeHeader.getEndOffset() -
              rangeHeader.getStartOffset() + 1;
        } else {
          length = sourceKeyDetails.getDataSize();
        }
        Long sourceKeyModificationTime = sourceKeyDetails
            .getModificationTime().toEpochMilli();
        String copySourceIfModifiedSince =
            getHeaders().getHeaderString(COPY_SOURCE_IF_MODIFIED_SINCE);
        String copySourceIfUnmodifiedSince =
            getHeaders().getHeaderString(COPY_SOURCE_IF_UNMODIFIED_SINCE);
        if (!checkCopySourceModificationTime(sourceKeyModificationTime,
            copySourceIfModifiedSince, copySourceIfUnmodifiedSince)) {
          throw newError(PRECOND_FAILED, sourceBucket + "/" + sourceKey);
        }

        try (OzoneInputStream sourceObject = sourceKeyDetails.getContent()) {
          long copyLength;
          if (range != null) {
            final long skipped =
                sourceObject.skip(rangeHeader.getStartOffset());
            if (skipped != rangeHeader.getStartOffset()) {
              throw new EOFException(
                  "Bytes to skip: "
                      + rangeHeader.getStartOffset() + " actual: " + skipped);
            }
          }
          try (OzoneOutputStream ozoneOutputStream = getClientProtocol()
              .createMultipartKey(volume.getName(), bucketName, key, length,
                  partNumber, uploadID)) {
            metadataLatencyNs =
                getMetrics().updateCopyKeyMetadataStats(startNanos);
            copyLength = IOUtils.copyLarge(sourceObject, ozoneOutputStream, 0, length,
                new byte[getIOBufferSize(length)]);
            ozoneOutputStream.getMetadata()
                .putAll(sourceKeyDetails.getMetadata());
            String raw = ozoneOutputStream.getMetadata().get(OzoneConsts.ETAG);
            if (raw != null) {
              ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, stripQuotes(raw));
            }
            outputStream = ozoneOutputStream;
          }
          getMetrics().incCopyObjectSuccessLength(copyLength);
          perf.appendSizeBytes(copyLength);
        }
      } else {
        long putLength;
        try (OzoneOutputStream ozoneOutputStream = getClientProtocol()
            .createMultipartKey(volume.getName(), bucketName, key, length,
                partNumber, uploadID)) {
          metadataLatencyNs =
              getMetrics().updatePutKeyMetadataStats(startNanos);
          putLength = IOUtils.copyLarge(multiDigestInputStream, ozoneOutputStream, 0, length,
              new byte[getIOBufferSize(length)]);
          byte[] digest = multiDigestInputStream.getMessageDigest(OzoneConsts.MD5_HASH).digest();
          ozoneOutputStream.getMetadata()
              .put(OzoneConsts.ETAG, DatatypeConverter.printHexBinary(digest).toLowerCase());
          outputStream = ozoneOutputStream;
        }
        getMetrics().incPutKeySuccessLength(putLength);
        perf.appendSizeBytes(putLength);
      }
      perf.appendMetaLatencyNanos(metadataLatencyNs);

      OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
          outputStream.getCommitUploadPartInfo();
      String eTag = omMultipartCommitUploadPartInfo.getETag();
      // If the OmMultipartCommitUploadPartInfo does not contain eTag,
      // fall back to MPU part name for compatibility in case the (old) OM
      // does not return the eTag field
      if (StringUtils.isEmpty(eTag)) {
        eTag = omMultipartCommitUploadPartInfo.getPartName();
      }
      eTag = wrapInQuotes(eTag);

      if (copyHeader != null) {
        getMetrics().updateCopyObjectSuccessStats(startNanos);
        return Response.ok(new CopyPartResult(eTag)).build();
      } else {
        getMetrics().updateCreateMultipartKeySuccessStats(startNanos);
        return Response.ok().header(HttpHeaders.ETAG, eTag).build();
      }

    } catch (OMException ex) {
      if (copyHeader != null) {
        getMetrics().updateCopyObjectFailureStats(startNanos);
      } else {
        getMetrics().updateCreateMultipartKeyFailureStats(startNanos);
      }
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName + "/" + key, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_PART) {
        OS3Exception os3Exception = newError(
            S3ErrorTable.INVALID_ARGUMENT, String.valueOf(partNumber), ex);
        os3Exception.setErrorMessage(ex.getMessage());
        throw os3Exception;
      }
      throw ex;
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (multiDigestInputStream != null) {
        multiDigestInputStream.resetDigests();
      }
    }
  }

  /**
   * Returns response for the listParts request.
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * @param ozoneBucket
   * @param key
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return
   * @throws IOException
   * @throws OS3Exception
   */
  private Response listParts(OzoneBucket ozoneBucket, String key, String uploadID,
      int partNumberMarker, int maxParts, PerformanceStringBuilder perf)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    ListPartsResponse listPartsResponse = new ListPartsResponse();
    String bucketName = ozoneBucket.getName();
    try {
      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          ozoneBucket.listParts(key, uploadID, partNumberMarker, maxParts);
      listPartsResponse.setBucket(bucketName);
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
        // If the ETag field does not exist, use MPU part name for backward
        // compatibility
        part.setETag(StringUtils.isNotEmpty(partInfo.getETag()) ?
            partInfo.getETag() : partInfo.getPartName());
        part.setSize(partInfo.getSize());
        part.setLastModified(Instant.ofEpochMilli(
            partInfo.getModificationTime()));
        listPartsResponse.addPart(part);
      });
    } catch (OMException ex) {
      getMetrics().updateListPartsFailureStats(startNanos);
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            bucketName + "/" + key + "/" + uploadID, ex);
      }
      throw ex;
    }
    long opLatencyNs = getMetrics().updateListPartsSuccessStats(startNanos);
    perf.appendCount(listPartsResponse.getPartList().size());
    perf.appendOpLatencyNanos(opLatencyNs);
    return Response.status(Status.OK).entity(listPartsResponse).build();
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  void copy(OzoneVolume volume, DigestInputStream src, long srcKeyLen,
      String destKey, String destBucket,
      ReplicationConfig replication,
      Map<String, String> metadata,
      PerformanceStringBuilder perf, long startNanos,
      Map<String, String> tags)
      throws IOException {
    long copyLength;
    if (datastreamEnabled && !(replication != null &&
        replication.getReplicationType() == EC) &&
        srcKeyLen > datastreamMinLength) {
      perf.appendStreamMode();
      copyLength = ObjectEndpointStreaming
          .copyKeyWithStream(volume.getBucket(destBucket), destKey, srcKeyLen,
              chunkSize, replication, metadata, src, perf, startNanos, tags);
    } else {
      try (OzoneOutputStream dest = getClientProtocol()
          .createKey(volume.getName(), destBucket, destKey, srcKeyLen,
              replication, metadata, tags)) {
        long metadataLatencyNs =
            getMetrics().updateCopyKeyMetadataStats(startNanos);
        perf.appendMetaLatencyNanos(metadataLatencyNs);
        copyLength = IOUtils.copyLarge(src, dest, 0, srcKeyLen, new byte[getIOBufferSize(srcKeyLen)]);
        String eTag = DatatypeConverter.printHexBinary(src.getMessageDigest().digest()).toLowerCase();
        dest.getMetadata().put(OzoneConsts.ETAG, eTag);
      }
    }
    getMetrics().incCopyObjectSuccessLength(copyLength);
    perf.appendSizeBytes(copyLength);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private CopyObjectResponse copyObject(OzoneVolume volume,
      String copyHeader, String destBucket, String destkey,
      ReplicationConfig replicationConfig, boolean storageTypeDefault,
      PerformanceStringBuilder perf)
      throws OS3Exception, IOException {
    long startNanos = Time.monotonicNowNanos();
    Pair<String, String> result = parseSourceHeader(copyHeader);

    String sourceBucket = result.getLeft();
    String sourceKey = result.getRight();
    DigestInputStream sourceDigestInputStream = null;

    if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
      String sourceBucketOwner = volume.getBucket(sourceBucket).getOwner();
      // The destBucket owner has already been checked in the caller method
      S3Owner.verifyBucketOwnerConditionOnCopyOperation(getHeaders(), sourceBucket, sourceBucketOwner, null, null);
    }
    try {
      OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
          volume.getName(), sourceBucket, sourceKey);
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
          copyObjectResponse.setETag(wrapInQuotes(sourceKeyDetails.getMetadata().get(OzoneConsts.ETAG)));
          copyObjectResponse.setLastModified(Instant.ofEpochMilli(
              Time.now()));
          return copyObjectResponse;
        }
      }
      long sourceKeyLen = sourceKeyDetails.getDataSize();

      // Object tagging in copyObject with tagging directive
      Map<String, String> tags;
      String tagCopyDirective = getHeaders().getHeaderString(TAG_DIRECTIVE_HEADER);
      if (StringUtils.isEmpty(tagCopyDirective) || tagCopyDirective.equals(CopyDirective.COPY.name())) {
        // Tag-set will be copied from the source directly
        tags = sourceKeyDetails.getTags();
      } else if (tagCopyDirective.equals(CopyDirective.REPLACE.name())) {
        // Replace the tags with the tags from the request headers
        tags = getTaggingFromHeaders(getHeaders());
      } else {
        OS3Exception ex = newError(INVALID_ARGUMENT, tagCopyDirective);
        ex.setErrorMessage("An error occurred (InvalidArgument) " +
            "when calling the CopyObject operation: " +
            "The tagging copy directive specified is invalid. Valid values are COPY or REPLACE.");
        throw ex;
      }

      // Custom metadata in copyObject with metadata directive
      Map<String, String> customMetadata;
      String metadataCopyDirective = getHeaders().getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER);
      if (StringUtils.isEmpty(metadataCopyDirective) || metadataCopyDirective.equals(CopyDirective.COPY.name())) {
        // The custom metadata will be copied from the source key
        customMetadata = sourceKeyDetails.getMetadata();
      } else if (metadataCopyDirective.equals(CopyDirective.REPLACE.name())) {
        // Replace the metadata with the metadata form the request headers
        customMetadata = getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());
      } else {
        OS3Exception ex = newError(INVALID_ARGUMENT, metadataCopyDirective);
        ex.setErrorMessage("An error occurred (InvalidArgument) " +
            "when calling the CopyObject operation: " +
            "The metadata copy directive specified is invalid. Valid values are COPY or REPLACE.");
        throw ex;
      }

      try (OzoneInputStream src = getClientProtocol().getKey(volume.getName(),
          sourceBucket, sourceKey)) {
        getMetrics().updateCopyKeyMetadataStats(startNanos);
        sourceDigestInputStream = new DigestInputStream(src, getMessageDigestInstance());
        copy(volume, sourceDigestInputStream, sourceKeyLen, destkey, destBucket, replicationConfig,
                customMetadata, perf, startNanos, tags);
      }

      final OzoneKeyDetails destKeyDetails = getClientProtocol().getKeyDetails(
          volume.getName(), destBucket, destkey);

      getMetrics().updateCopyObjectSuccessStats(startNanos);
      CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
      copyObjectResponse.setETag(wrapInQuotes(destKeyDetails.getMetadata().get(OzoneConsts.ETAG)));
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
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (sourceDigestInputStream != null) {
        sourceDigestInputStream.getMessageDigest().reset();
      }
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

  public static boolean checkCopySourceModificationTime(
      Long lastModificationTime,
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

  private Response putObjectTagging(OzoneBucket bucket, String keyName, InputStream body)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3Tagging tagging = null;
    try {
      tagging = new PutTaggingUnmarshaller().readFrom(body);
      tagging.validate();
    } catch (Exception ex) {
      OS3Exception exception = S3ErrorTable.newError(S3ErrorTable.MALFORMED_XML, keyName);
      exception.setErrorMessage(exception.getErrorMessage() + ". " + ex.getMessage());
      throw exception;
    }

    Map<String, String> tags = validateAndGetTagging(
        tagging.getTagSet().getTags(), // Nullity check was done in previous parsing step
        Tag::getKey,
        Tag::getValue
    );

    try {
      bucket.putObjectTagging(keyName, tags);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.INVALID_REQUEST) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, keyName);
      } else if (ex.getResult() == ResultCodes.PERMISSION_DENIED) {
        throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, keyName);
      } else if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_KEY, keyName);
      } else if (ex.getResult() == ResultCodes.NOT_SUPPORTED_OPERATION) {
        // When putObjectTagging operation is applied on FSO directory
        throw S3ErrorTable.newError(S3ErrorTable.NOT_IMPLEMENTED, keyName);
      }
      throw ex;
    }
    getMetrics().updatePutObjectTaggingSuccessStats(startNanos);
    return Response.ok().build();
  }

  private Response getObjectTagging(OzoneBucket bucket, String keyName) throws IOException {
    long startNanos = Time.monotonicNowNanos();

    Map<String, String> tagMap = bucket.getObjectTagging(keyName);

    getMetrics().updateGetObjectTaggingSuccessStats(startNanos);
    return Response.ok(S3Tagging.fromMap(tagMap), MediaType.APPLICATION_XML_TYPE).build();
  }

  private Response deleteObjectTagging(OzoneVolume volume, String bucketName, String keyName)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();

    try {
      volume.getBucket(bucketName).deleteObjectTagging(keyName);
    } catch (OMException ex) {
      // Unlike normal key deletion that ignores the key not found exception
      // DeleteObjectTagging should throw the exception if the key does not exist
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_KEY, keyName);
      }
      throw ex;
    }

    getMetrics().updateDeleteObjectTaggingSuccessStats(startNanos);
    return Response.noContent().build();
  }

  @VisibleForTesting
  public boolean isDatastreamEnabled() {
    return datastreamEnabled;
  }

  @VisibleForTesting
  public MessageDigest getMessageDigestInstance() {
    return E_TAG_PROVIDER.get();
  }

  @VisibleForTesting
  public MessageDigest getSha256DigestInstance() {
    return SHA_256_PROVIDER.get();
  }

  private String extractPartsCount(String eTag) {
    if (eTag.contains("-")) {
      String[] parts = eTag.replace("\"", "").split("-");
      String lastPart = parts[parts.length - 1];
      return lastPart.matches("\\d+") ? lastPart : null;
    }
    return null;
  }

  private int getIOBufferSize(long fileLength) {
    if (bufferSize == 0) {
      // this is mainly for unit tests as init() will not be called in the unit tests
      LOG.warn("buffer size is set to {}", IOUtils.DEFAULT_BUFFER_SIZE);
      bufferSize = IOUtils.DEFAULT_BUFFER_SIZE;
    }
    if (fileLength == 0) {
      // for empty file
      return bufferSize;
    } else {
      return fileLength < bufferSize ? (int) fileLength : bufferSize;
    }
  }

  /**
   * Create a {@link S3ChunkInputStreamInfo} that contains the necessary information to handle
   * the S3 chunk upload.
   */
  private S3ChunkInputStreamInfo getS3ChunkInputStreamInfo(
      InputStream body, long contentLength, String amzDecodedLength, String keyPath) throws OS3Exception {
    final String amzContentSha256Header = validateSignatureHeader(getHeaders(), keyPath, signatureInfo.isSignPayload());
    final InputStream chunkInputStream;
    final long effectiveLength;
    if (hasMultiChunksPayload(amzContentSha256Header)) {
      validateMultiChunksUpload(getHeaders(), amzDecodedLength, keyPath);
      if (hasUnsignedPayload(amzContentSha256Header)) {
        chunkInputStream = new UnsignedChunksInputStream(body);
      } else {
        chunkInputStream = new SignedChunksInputStream(body);
      }
      effectiveLength = Long.parseLong(amzDecodedLength);
    } else {
      // Single chunk upload: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
      // Possible x-amz-content-sha256 header values
      // - Actual payload checksum value: For signed payload
      // - UNSIGNED-PAYLOAD: For unsigned payload
      chunkInputStream = body;
      effectiveLength = contentLength;
    }

    // MessageDigest is used for ETag calculation
    // and Sha256Digest is used for "x-amz-content-sha256" header verification
    List<MessageDigest> digests = new ArrayList<>();
    digests.add(getMessageDigestInstance());
    if (!hasUnsignedPayload(amzContentSha256Header) && !hasMultiChunksPayload(amzContentSha256Header)) {
      digests.add(getSha256DigestInstance());
    }
    MultiDigestInputStream multiDigestInputStream =
        new MultiDigestInputStream(chunkInputStream, digests);
    return new S3ChunkInputStreamInfo(multiDigestInputStream, effectiveLength);
  }

  @Immutable
  static final class S3ChunkInputStreamInfo {
    private final MultiDigestInputStream multiDigestInputStream;
    private final long effectiveLength;

    S3ChunkInputStreamInfo(MultiDigestInputStream multiDigestInputStream, long effectiveLength) {
      this.multiDigestInputStream = multiDigestInputStream;
      this.effectiveLength = effectiveLength;
    }

    public MultiDigestInputStream getMultiDigestInputStream() {
      return multiDigestInputStream;
    }

    public long getEffectiveLength() {
      return effectiveLength;
    }
  }
}
