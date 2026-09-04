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

import static org.apache.hadoop.ozone.OzoneConsts.MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MAX_PARTS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.OBJECT_ATTRIBUTES_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.PART_NUMBER_MARKER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.S3HeadObjectAttributes;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the GetObjectAttributes S3 API ({@code GET /{bucket}/{key}?attributes}).
 *
 * <p>Returns selected metadata about an object without transferring the object body.
 * Supported attributes: {@code ETag}, {@code ObjectSize}, {@code StorageClass}, {@code ObjectParts}.
 *
 * <p>The {@code Checksum} attribute is not yet supported because Ozone does not store
 * non-MD5 checksum algorithms in key metadata. For general-purpose buckets, {@code Part}
 * elements under {@code ObjectParts} are omitted unless an additional checksum is stored
 * on the object, matching AWS S3 behavior. Object versioning ({@code versionId}) and
 * SSE-C encryption headers are also not supported and are silently ignored.
 *
 * <p>See https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
 */
class ObjectAttributesHandler extends ObjectOperationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectAttributesHandler.class);

  /** Valid values for the x-amz-object-attributes request header. */
  static final String ATTR_ETAG = "ETag";
  static final String ATTR_CHECKSUM = "Checksum";
  static final String ATTR_OBJECT_PARTS = "ObjectParts";
  static final String ATTR_STORAGE_CLASS = "StorageClass";
  static final String ATTR_OBJECT_SIZE = "ObjectSize";

  private static final Set<String> KNOWN_ATTRIBUTES = new HashSet<>(Arrays.asList(
      ATTR_ETAG, ATTR_CHECKSUM, ATTR_OBJECT_PARTS, ATTR_STORAGE_CLASS, ATTR_OBJECT_SIZE));

  private static final String ADDITIONAL_CHECKSUM_METADATA_PREFIX = "x-amz-checksum-";

  /**
   * Returns whether key metadata contains a stored AWS additional checksum.
   * Ozone does not persist {@code x-amz-checksum-*} on upload today, so this is
   * false for normal objects until checksum storage is implemented.
   */
  static boolean hasStoredAdditionalChecksum(OzoneKey key) {
    if (key == null || key.getMetadata() == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : key.getMetadata().entrySet()) {
      String name = entry.getKey();
      if (name != null
          && name.toLowerCase(Locale.ROOT).startsWith(ADDITIONAL_CHECKSUM_METADATA_PREFIX)
          && StringUtils.isNotBlank(entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  @Override
  Response handleGetRequest(ObjectRequestContext context, String keyPath)
      throws IOException, OS3Exception {

    if (queryParams().get(QueryParams.ATTRIBUTES) == null) {
      return null;
    }

    context.setAction(S3GAction.GET_OBJECT_ATTRIBUTES);

    final long startNanos = context.getStartNanos();
    try {
      Set<String> requestedAttributes = parseAttributesHeader(keyPath);
      String bucketName = context.getBucketName();

      OzoneKey key;
      NavigableMap<Integer, Long> completedPartSizes = null;
      try {
        if (requestedAttributes.contains(ATTR_OBJECT_PARTS)) {
          S3HeadObjectAttributes headAttributes =
              getClientProtocol().headS3ObjectAttributes(bucketName, keyPath);
          key = headAttributes.getKey();
          completedPartSizes = headAttributes.getCompletedMultipartPartSizes();
        } else {
          key = getClientProtocol().headS3Object(bucketName, keyPath);
        }
        validateFileKey(keyPath, key);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
          throw newError(NO_SUCH_KEY, keyPath, ex);
        } else if (isAccessDenied(ex)) {
          throw newError(ACCESS_DENIED, bucketName + "/" + keyPath, ex);
        }
        throw ex;
      }

      GetObjectAttributesResponse response =
          buildResponse(keyPath, key, requestedAttributes, completedPartSizes);

      Response.ResponseBuilder rb = Response.ok(response, MediaType.APPLICATION_XML_TYPE);
      ObjectEndpoint.addLastModifiedDate(rb, key);
      getMetrics().updateGetObjectAttributesSuccessStats(startNanos);
      return rb.build();

    } catch (OS3Exception | IOException ex) {
      getMetrics().updateGetObjectAttributesFailureStats(startNanos);
      throw ex;
    }
  }

  /**
   * Parses and validates the {@code x-amz-object-attributes} request header.
   *
   * <p>The header is required. Unknown attribute names that are not in the S3-defined
   * set are rejected with {@code InvalidArgument}.
   */
  private Set<String> parseAttributesHeader(String keyPath) throws OS3Exception {
    String headerValue = getHeaders().getHeaderString(OBJECT_ATTRIBUTES_HEADER);
    if (StringUtils.isBlank(headerValue)) {
      throw newError(INVALID_ARGUMENT, keyPath,
          new IllegalArgumentException(OBJECT_ATTRIBUTES_HEADER + " is required"));
    }

    Set<String> requested = new HashSet<>();
    for (String token : headerValue.split(",")) {
      String attr = token.trim();
      if (!KNOWN_ATTRIBUTES.contains(attr)) {
        throw newError(INVALID_ARGUMENT, keyPath,
            new IllegalArgumentException("Invalid value for " + OBJECT_ATTRIBUTES_HEADER + ": " + attr));
      }
      requested.add(attr);
    }
    return requested;
  }

  private GetObjectAttributesResponse buildResponse(String keyPath, OzoneKey key,
      Set<String> requested, NavigableMap<Integer, Long> completedPartSizes)
      throws IOException, OS3Exception {
    GetObjectAttributesResponse resp = new GetObjectAttributesResponse();

    if (requested.contains(ATTR_ETAG)) {
      String eTag = key.getMetadata().get(OzoneConsts.ETAG);
      if (eTag != null) {
        resp.setETag(eTag);
      }
    }

    if (requested.contains(ATTR_OBJECT_SIZE)) {
      resp.setObjectSize(key.getDataSize());
    }

    if (requested.contains(ATTR_STORAGE_CLASS)) {
      S3StorageType storageType = key.getReplicationConfig() == null
          ? S3StorageType.STANDARD
          : S3StorageType.fromReplicationConfig(key.getReplicationConfig());
      resp.setStorageClass(storageType.toString());
    }

    if (requested.contains(ATTR_OBJECT_PARTS)) {
      String eTag = key.getMetadata().get(OzoneConsts.ETAG);
      if (eTag != null) {
        String partsCountStr = extractPartsCount(eTag);
        if (partsCountStr != null && completedPartSizes != null) {
          resp.setObjectParts(buildObjectParts(keyPath, Integer.parseInt(partsCountStr),
              completedPartSizes, key));
        }
      }
    }

    // ATTR_CHECKSUM is intentionally not populated: Ozone does not store non-MD5
    // checksum algorithms in key metadata. The field is simply omitted from the
    // response, which is valid per the S3 spec.

    return resp;
  }

  /**
   * Builds the {@link GetObjectAttributesResponse.ObjectParts} element for a completed
   * multipart object, including per-part sizes and optional pagination.
   *
   * <p>When {@code x-amz-max-parts} is omitted, the page size defaults to 1000, matching ListParts.
   * Part numbers and sizes are paginated over the actual part numbers from OM, which may be
   * non-contiguous. {@code TotalPartsCount} follows the block-derived part count when it differs
   * from the ETag suffix.
   *
   * <p>For general-purpose buckets, individual {@code Part} elements are returned only when the
   * object has a stored AWS additional checksum in key metadata; otherwise only {@code ObjectParts}
   * summary and pagination fields are returned.
   */
  private GetObjectAttributesResponse.ObjectParts buildObjectParts(String keyPath,
      int totalPartsCount, NavigableMap<Integer, Long> partSizes, OzoneKey key)
      throws OS3Exception {
    int maxParts = parseMaxPartsHeader(keyPath);
    int marker = parsePartNumberMarkerHeader(keyPath);
    boolean partNumberMarkerSet = isPartNumberMarkerHeaderSet();

    GetObjectAttributesResponse.ObjectParts parts = new GetObjectAttributesResponse.ObjectParts();
    int partsCount = totalPartsCount;
    if (partSizes.size() != totalPartsCount) {
      LOG.debug("ETag parts count {} differs from block-derived part count {} for key {}",
          totalPartsCount, partSizes.size(), keyPath);
      partsCount = partSizes.size();
    }
    parts.setPartsCount(partsCount);
    parts.setMaxParts(maxParts);
    if (partNumberMarkerSet) {
      parts.setPartNumberMarker(marker);
    }

    Iterator<Map.Entry<Integer, Long>> partIterator =
        partSizes.tailMap(marker, false).entrySet().iterator();
    // TODO: For FSO (directory) buckets, always include Part entries per AWS
    // directory-bucket GetObjectAttributes behavior, regardless of checksum metadata.
    boolean includePartEntries = hasStoredAdditionalChecksum(key);
    Integer lastPartReturned = null;
    int partsOnPage = 0;
    while (partIterator.hasNext() && partsOnPage < maxParts) {
      Map.Entry<Integer, Long> partEntry = partIterator.next();
      if (includePartEntries) {
        parts.addPart(new GetObjectAttributesResponse.Part(
            partEntry.getKey(), partEntry.getValue()));
      }
      lastPartReturned = partEntry.getKey();
      partsOnPage++;
    }

    boolean truncated = partIterator.hasNext();
    parts.setTruncated(truncated);
    if (truncated && lastPartReturned != null) {
      parts.setNextPartNumberMarker(lastPartReturned);
    }
    return parts;
  }

  private int parseMaxPartsHeader(String resource) throws OS3Exception {
    return parseMaxPartsHeader(resource, GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT);
  }

  private int parseMaxPartsHeader(String resource, int defaultValue) throws OS3Exception {
    String headerValue = getHeaders().getHeaderString(MAX_PARTS_HEADER);
    if (StringUtils.isBlank(headerValue)) {
      return defaultValue;
    }
    try {
      int maxParts = Integer.parseInt(headerValue.trim());
      if (maxParts <= 0 || maxParts > GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT) {
        throw newError(INVALID_ARGUMENT, resource,
            new IllegalArgumentException("max-parts must be between 1 and "
                + GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT));
      }
      return maxParts;
    } catch (NumberFormatException ex) {
      throw newError(INVALID_ARGUMENT, resource, ex);
    }
  }

  private boolean isPartNumberMarkerHeaderSet() {
    return StringUtils.isNotBlank(getHeaders().getHeaderString(PART_NUMBER_MARKER_HEADER));
  }

  private int parsePartNumberMarkerHeader(String resource) throws OS3Exception {
    String headerValue = getHeaders().getHeaderString(PART_NUMBER_MARKER_HEADER);
    if (StringUtils.isBlank(headerValue)) {
      return 0;
    }
    try {
      int marker = Integer.parseInt(headerValue.trim());
      if (marker < 0 || marker > MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD) {
        throw newError(INVALID_ARGUMENT, resource,
            new IllegalArgumentException("part-number-marker must be between 0 and "
                + MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD));
      }
      return marker;
    } catch (NumberFormatException ex) {
      throw newError(INVALID_ARGUMENT, resource, ex);
    }
  }
}
