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

package org.apache.hadoop.ozone.s3.exception;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.ozone.OzoneConsts.S3_REQUEST_HEADER_METADATA_SIZE_LIMIT_KB;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_NOT_SATISFIABLE;

import jakarta.annotation.Nullable;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents errors from Ozone S3 service.
 * This class needs to be updated to add new errors when required.
 */
public enum S3ErrorTable {

  INVALID_URI("InvalidURI",
      "Couldn't parse the specified URI.", HTTP_BAD_REQUEST),

  NO_SUCH_VOLUME("NoSuchVolume",
      "The specified volume does not exist", HTTP_NOT_FOUND),

  NO_SUCH_BUCKET("NoSuchBucket",
      "The specified bucket does not exist", HTTP_NOT_FOUND),

  AUTH_PROTOCOL_NOT_SUPPORTED("AuthProtocolNotSupported",
      "Auth protocol used for this request is not supported.", HTTP_BAD_REQUEST),

  S3_AUTHINFO_CREATION_ERROR("InvalidRequest", "Error creating s3 auth info. " +
          "The request may not be signed using AWS V4 signing algorithm," +
          " or might be invalid", HTTP_FORBIDDEN),

  BUCKET_NOT_EMPTY(
      "BucketNotEmpty", "The bucket you tried to delete is not empty. " +
      "If you are using --force option to delete all objects in the bucket, " +
      "please ensure that the bucket layout is OBJECT_STORE or " +
      "that the bucket is completely empty before delete.",
      HTTP_CONFLICT),

  MALFORMED_HEADER(
      "AuthorizationHeaderMalformed", "The authorization header you provided " +
      "is invalid.", HTTP_BAD_REQUEST),

  NO_SUCH_KEY(
      "NoSuchKey", "The specified key does not exist", HTTP_NOT_FOUND),

  INVALID_ARGUMENT(
      "InvalidArgument", "Invalid Argument", HTTP_BAD_REQUEST),

  INVALID_REQUEST(
      "InvalidRequest", "Invalid Request", HTTP_BAD_REQUEST),

  INVALID_RANGE(
      "InvalidRange", "The requested range is not satisfiable",
      RANGE_NOT_SATISFIABLE),

  NO_SUCH_UPLOAD(
      "NoSuchUpload", "The specified multipart upload does not exist. The " +
      "upload ID might be invalid, or the multipart upload might have " +
      "been aborted or completed.", HTTP_NOT_FOUND),

  INVALID_BUCKET_NAME(
      "InvalidBucketName", "The specified bucket is not valid.",
      HTTP_BAD_REQUEST),

  INVALID_PART(
      "InvalidPart", "One or more of the specified parts could not be found." +
      " The part might not have been uploaded, or the specified entity " +
      "tag might not have matched the part's entity tag.", HTTP_BAD_REQUEST),

  INVALID_PART_ORDER(
      "InvalidPartOrder", "The list of parts was not in ascending order. The " +
      "parts list must be specified in order by part number.",
      HTTP_BAD_REQUEST),

  ENTITY_TOO_SMALL(
      "EntityTooSmall", "Your proposed upload is smaller than the minimum " +
      "allowed object size. Each part must be at least 5 MB in size, except " +
      "the last part.", HTTP_BAD_REQUEST),

  INTERNAL_ERROR(
      "InternalError", "We encountered an internal error. Please try again.",
      HTTP_INTERNAL_ERROR),

  ACCESS_DENIED(
      "AccessDenied", "User doesn't have the right to access this " +
      "resource.", HTTP_FORBIDDEN),

  PRECOND_FAILED(
      "PreconditionFailed", "At least one of the pre-conditions you " +
      "specified did not hold", HTTP_PRECON_FAILED),

  CONDITIONAL_REQUEST_CONFLICT("ConditionalRequestConflict",
          "A conflicting conditional operation occurred. Retry the request.",
          HTTP_CONFLICT),
  
  NOT_IMPLEMENTED(
      "NotImplemented", "This part of feature is not implemented yet.",
      HTTP_NOT_IMPLEMENTED),

  NO_OVERWRITE(
      "Conflict", "Cannot overwrite file with directory", HTTP_CONFLICT),

  METADATA_TOO_LARGE(
      "MetadataTooLarge", "Illegal user defined metadata. Combined size " +
      "exceeds the maximum allowed metadata size of " +
      S3_REQUEST_HEADER_METADATA_SIZE_LIMIT_KB + "KB", HTTP_BAD_REQUEST),

  BUCKET_ALREADY_EXISTS(
      "BucketAlreadyExists", "The requested bucket name is not available" +
      " as it already exists.", HTTP_CONFLICT),

  INVALID_TAG(
      "InvalidTag", "Your request contains tag input that is not valid.", HTTP_BAD_REQUEST),

  NO_SUCH_TAG_SET(
      "NoSuchTagSet", "The specified tag does not exist.", HTTP_NOT_FOUND),

  MALFORMED_XML(
      "MalformedXML", "The XML you provided was not well-formed or did not " +
      "validate against our published schema", HTTP_BAD_REQUEST),

  QUOTA_EXCEEDED(
      "QuotaExceeded", "The quota has been exceeded. " +
      "Please review your disk space or namespace usage and adjust accordingly.",
      HTTP_FORBIDDEN
  ),

  INVALID_STORAGE_CLASS(
      "InvalidStorageClass", "The storage class that you specified is not valid. " +
      "Provide a supported storage class[STANDARD|REDUCED_REDUNDANCY|STANDARD_IA] or " +
      "a valid custom EC storage config for if using STANDARD_IA.",
      HTTP_BAD_REQUEST),

  BUCKET_OWNER_MISMATCH(
      "Access Denied", "User doesn't have permission to access this resource due to a " +
      "bucket ownership mismatch.", HTTP_FORBIDDEN),

  X_AMZ_CONTENT_SHA256_MISMATCH(
      "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does " +
      "not match the computed hash.", HTTP_BAD_REQUEST),

  BAD_DIGEST(
      "BadDigest", "The Content-MD5 or checksum value that you specified did not match what the server received.",
      HTTP_BAD_REQUEST),

  INVALID_DIGEST(
      "InvalidDigest", "The Content-MD5 you specified is not valid.", HTTP_BAD_REQUEST);

  private static final Logger LOG = LoggerFactory.getLogger(S3ErrorTable.class);

  private final String code;
  private final String message;
  private final int httpCode;

  S3ErrorTable(String code, String message, int httpCode) {
    this.code = code;
    this.message = message;
    this.httpCode = httpCode;
  }

  public String getCode() {
    return code;
  }

  public String getErrorMessage() {
    return message;
  }

  public int getHttpCode() {
    return httpCode;
  }

  /** Converts result code of {@code OMException} to {@code S3ErrorTable}, which
   * should be thrown via {@link #newError(S3ErrorTable, String, Exception)}. */
  public static S3ErrorTable translateResultCode(OMException ex) {
    switch (ex.getResult()) {
    case ACCESS_DENIED:
    case INVALID_TOKEN:
    case PERMISSION_DENIED:
      return ACCESS_DENIED;
    case ATOMIC_WRITE_CONFLICT:
      return CONDITIONAL_REQUEST_CONFLICT;
    case BUCKET_ALREADY_EXISTS:
      return BUCKET_ALREADY_EXISTS;
    case BUCKET_NOT_EMPTY:
      return BUCKET_NOT_EMPTY;
    case BUCKET_NOT_FOUND:
    case VOLUME_NOT_FOUND:
      return NO_SUCH_BUCKET;
    case ENTITY_TOO_SMALL:
      return ENTITY_TOO_SMALL;
    case ETAG_MISMATCH:
    case ETAG_NOT_AVAILABLE:
    case KEY_ALREADY_EXISTS:
      return PRECOND_FAILED;
    case FILE_ALREADY_EXISTS:
      return NO_OVERWRITE;
    case INVALID_BUCKET_NAME:
      return INVALID_BUCKET_NAME;
    case INVALID_PART:
      return INVALID_PART;
    case INVALID_PART_ORDER:
      return INVALID_PART_ORDER;
    case INVALID_REQUEST:
      return INVALID_REQUEST;
    case KEY_NOT_FOUND:
      return NO_SUCH_KEY;
    case NOT_SUPPORTED_OPERATION:
      return NOT_IMPLEMENTED;
    case QUOTA_EXCEEDED:
      return QUOTA_EXCEEDED;
    default:
      return INTERNAL_ERROR;
    }
  }

  /** Same as {@link #newError(String, OMException)}, but uses {@code bucket} as the {@code resource}
   * in case of {@link #NO_SUCH_BUCKET} error. */
  public static OS3Exception newError(String bucket, String resource, OMException cause) {
    S3ErrorTable errorCode = translateResultCode(cause);
    return new OS3Exception(errorCode, cause, NO_SUCH_BUCKET == errorCode ? bucket : resource);
  }

  /** Creates new {@link OS3Exception} for {@link OMException} and {@code resource}. */
  public static OS3Exception newError(@Nullable String resource, OMException cause) {
    return new OS3Exception(translateResultCode(cause), cause, resource);
  }

  /** Creates new {@link OS3Exception} for {@link S3ErrorTable}. */
  public static OS3Exception newError(S3ErrorTable errorCode) {
    return new OS3Exception(errorCode, null, null);
  }

  /** Creates new {@link OS3Exception} for {@link S3ErrorTable} and {@code resource}. */
  public static OS3Exception newError(S3ErrorTable errorCode, @Nullable String resource) {
    return new OS3Exception(errorCode, null, resource);
  }

  /** Creates new {@link OS3Exception} for {@link S3ErrorTable} and {@code cause}. */
  public static OS3Exception newError(S3ErrorTable errorCode, Exception cause) {
    return new OS3Exception(errorCode, cause, null);
  }

  /** Creates new {@link OS3Exception} for {@link S3ErrorTable}, {@code resource} and {@code cause}. */
  public static OS3Exception newError(S3ErrorTable errorCode, @Nullable String resource, Exception cause) {
    return new OS3Exception(errorCode, cause, resource);
  }

  static void log(OS3Exception err) {
    if (err.getHttpCode() == HTTP_INTERNAL_ERROR) {
      LOG.error("Internal Error: {}", err.toXml(), err.getCause());
    } else if (LOG.isDebugEnabled()) {
      LOG.debug(err.toXml(), err.getCause());
    }
  }
}
