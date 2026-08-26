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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.OBJECT_ATTRIBUTES_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3StorageType;

/**
 * Handles the GetObjectAttributes S3 API ({@code GET /{bucket}/{key}?attributes}).
 *
 * <p>Returns selected metadata about an object without transferring the object body.
 * Supported attributes: {@code ETag}, {@code ObjectSize}, {@code StorageClass}, {@code ObjectParts}.
 *
 * <p>The {@code Checksum} attribute is not yet supported because Ozone does not store
 * non-MD5 checksum algorithms in key metadata. Object versioning ({@code versionId}) and
 * SSE-C encryption headers are also not supported and are silently ignored.
 *
 * <p>See https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
 */
class ObjectAttributesHandler extends ObjectOperationHandler {

  /** Valid values for the x-amz-object-attributes request header. */
  static final String ATTR_ETAG = "ETag";
  static final String ATTR_CHECKSUM = "Checksum";
  static final String ATTR_OBJECT_PARTS = "ObjectParts";
  static final String ATTR_STORAGE_CLASS = "StorageClass";
  static final String ATTR_OBJECT_SIZE = "ObjectSize";

  private static final Set<String> KNOWN_ATTRIBUTES = new HashSet<>(Arrays.asList(
      ATTR_ETAG, ATTR_CHECKSUM, ATTR_OBJECT_PARTS, ATTR_STORAGE_CLASS, ATTR_OBJECT_SIZE));

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
      try {
        key = getClientProtocol().headS3Object(bucketName, keyPath);
        validateFileKey(keyPath, key);
      } catch (OMException ex) {
        if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
          throw newError(NO_SUCH_KEY, keyPath, ex);
        } else if (isAccessDenied(ex)) {
          throw newError(ACCESS_DENIED, bucketName + "/" + keyPath, ex);
        }
        throw ex;
      }

      GetObjectAttributesResponse response = buildResponse(key, requestedAttributes);

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

  private GetObjectAttributesResponse buildResponse(OzoneKey key, Set<String> requested) {
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
        if (partsCountStr != null) {
          GetObjectAttributesResponse.ObjectParts parts = new GetObjectAttributesResponse.ObjectParts();
          parts.setPartsCount(Integer.parseInt(partsCountStr));
          parts.setTruncated(false);
          resp.setObjectParts(parts);
        }
      }
    }

    // ATTR_CHECKSUM is intentionally not populated: Ozone does not store non-MD5
    // checksum algorithms in key metadata. The field is simply omitted from the
    // response, which is valid per the S3 spec.

    return resp;
  }
}
