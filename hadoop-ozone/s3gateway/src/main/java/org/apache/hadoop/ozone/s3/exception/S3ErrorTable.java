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

import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents errors from Ozone S3 service.
 * This class needs to be updated to add new errors when required.
 */
public final class S3ErrorTable {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3ErrorTable.class);

  public static final OS3Exception INVALID_URI = new OS3Exception("InvalidURI",
      "Couldn't parse the specified URI.", HTTP_BAD_REQUEST);

  public static final OS3Exception NO_SUCH_VOLUME = new OS3Exception(
      "NoSuchVolume", "The specified volume does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception NO_SUCH_BUCKET = new OS3Exception(
      "NoSuchBucket", "The specified bucket does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception AUTH_PROTOCOL_NOT_SUPPORTED =
      new OS3Exception("AuthProtocolNotSupported", "Auth protocol used for" +
          " this request is not supported.", HTTP_BAD_REQUEST);

  public static final OS3Exception S3_AUTHINFO_CREATION_ERROR =
      new OS3Exception("InvalidRequest", "Error creating s3 auth info. " +
          "The request may not be signed using AWS V4 signing algorithm," +
          " or might be invalid", HTTP_FORBIDDEN);

  public static final OS3Exception BUCKET_NOT_EMPTY = new OS3Exception(
      "BucketNotEmpty", "The bucket you tried to delete is not empty. " +
      "If you are using --force option to delete all objects in the bucket, " +
      "please ensure that the bucket layout is OBJECT_STORE or " +
      "that the bucket is completely empty before delete.",
      HTTP_CONFLICT);

  public static final OS3Exception MALFORMED_HEADER = new OS3Exception(
      "AuthorizationHeaderMalformed", "The authorization header you provided " +
      "is invalid.", HTTP_BAD_REQUEST);

  public static final OS3Exception NO_SUCH_KEY = new OS3Exception(
      "NoSuchKey", "The specified key does not exist", HTTP_NOT_FOUND);

  public static final OS3Exception INVALID_ARGUMENT = new OS3Exception(
      "InvalidArgument", "Invalid Argument", HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_REQUEST = new OS3Exception(
      "InvalidRequest", "Invalid Request", HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_RANGE = new OS3Exception(
      "InvalidRange", "The requested range is not satisfiable",
      RANGE_NOT_SATISFIABLE);

  public static final OS3Exception NO_SUCH_UPLOAD = new OS3Exception(
      "NoSuchUpload", "The specified multipart upload does not exist. The " +
      "upload ID might be invalid, or the multipart upload might have " +
      "been aborted or completed.", HTTP_NOT_FOUND);

  public static final OS3Exception INVALID_BUCKET_NAME = new OS3Exception(
      "InvalidBucketName", "The specified bucket is not valid.",
      HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_PART = new OS3Exception(
      "InvalidPart", "One or more of the specified parts could not be found." +
      " The part might not have been uploaded, or the specified entity " +
      "tag might not have matched the part's entity tag.", HTTP_BAD_REQUEST);

  public static final OS3Exception INVALID_PART_ORDER = new OS3Exception(
      "InvalidPartOrder", "The list of parts was not in ascending order. The " +
      "parts list must be specified in order by part number.",
      HTTP_BAD_REQUEST);

  public static final OS3Exception ENTITY_TOO_SMALL = new OS3Exception(
      "EntityTooSmall", "Your proposed upload is smaller than the minimum " +
      "allowed object size. Each part must be at least 5 MB in size, except " +
      "the last part.", HTTP_BAD_REQUEST);

  public static final OS3Exception INTERNAL_ERROR = new OS3Exception(
      "InternalError", "We encountered an internal error. Please try again.",
      HTTP_INTERNAL_ERROR);

  public static final OS3Exception ACCESS_DENIED = new OS3Exception(
      "AccessDenied", "User doesn't have the right to access this " +
      "resource.", HTTP_FORBIDDEN);

  public static final OS3Exception PRECOND_FAILED = new OS3Exception(
      "PreconditionFailed", "At least one of the pre-conditions you " +
      "specified did not hold", HTTP_PRECON_FAILED);
  
  public static final OS3Exception NOT_IMPLEMENTED = new OS3Exception(
      "NotImplemented", "This part of feature is not implemented yet.",
      HTTP_NOT_IMPLEMENTED);

  public static final OS3Exception NO_OVERWRITE = new OS3Exception(
      "Conflict", "Cannot overwrite file with directory", HTTP_CONFLICT);

  public static final OS3Exception METADATA_TOO_LARGE = new OS3Exception(
      "MetadataTooLarge", "Illegal user defined metadata. Combined size " +
      "exceeds the maximum allowed metadata size of " +
      S3_REQUEST_HEADER_METADATA_SIZE_LIMIT_KB + "KB", HTTP_BAD_REQUEST);

  public static final OS3Exception BUCKET_ALREADY_EXISTS = new OS3Exception(
      "BucketAlreadyExists", "The requested bucket name is not available" +
      " as it already exists.", HTTP_CONFLICT);

  public static final OS3Exception INVALID_TAG = new OS3Exception(
      "InvalidTag", "Your request contains tag input that is not valid.", HTTP_BAD_REQUEST);

  public static final OS3Exception NO_SUCH_TAG_SET = new OS3Exception(
      "NoSuchTagSet", "The specified tag does not exist.", HTTP_NOT_FOUND);

  public static final OS3Exception MALFORMED_XML = new OS3Exception(
      "MalformedXML", "The XML you provided was not well-formed or did not " +
      "validate against our published schema", HTTP_BAD_REQUEST);

  public static final OS3Exception QUOTA_EXCEEDED = new OS3Exception(
      "QuotaExceeded", "The quota has been exceeded. " +
      "Please review your disk space or namespace usage and adjust accordingly.",
      HTTP_FORBIDDEN
  );

  public static final OS3Exception INVALID_STORAGE_CLASS = new OS3Exception(
      "InvalidStorageClass", "The storage class that you specified is not valid. " +
      "Provide a supported storage class[STANDARD|REDUCED_REDUNDANCY|STANDARD_IA] or " +
      "a valid custom EC storage config for if using STANDARD_IA.",
      HTTP_BAD_REQUEST);

  public static final OS3Exception BUCKET_OWNER_MISMATCH = new OS3Exception(
      "Access Denied", "User doesn't have permission to access this resource due to a " +
      "bucket ownership mismatch.", HTTP_FORBIDDEN);

  public static final OS3Exception X_AMZ_CONTENT_SHA256_MISMATCH = new OS3Exception(
      "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does " +
      "not match the computed hash.", HTTP_BAD_REQUEST);

  private static Function<Exception, OS3Exception> generateInternalError =
      e -> new OS3Exception("InternalError", e.getMessage(), HTTP_INTERNAL_ERROR);

  private S3ErrorTable() {
    //No one should construct this object.
  }

  public static OS3Exception newError(OS3Exception e, String resource) {
    return newError(e, resource, null);
  }

  /**
   * Create a new instance of Error.
   * @param e Error Template
   * @param resource Resource associated with this exception
   * @param ex the original exception, may be null
   * @return creates a new instance of error based on the template
   */
  public static OS3Exception newError(OS3Exception e, String resource,
      Exception ex) {
    OS3Exception err =  new OS3Exception(e.getCode(), e.getErrorMessage(),
        e.getHttpCode());
    err.setResource(resource);
    if (e.getHttpCode() == HTTP_INTERNAL_ERROR) {
      LOG.error("Internal Error: {}", err.toXml(), ex);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug(err.toXml(), ex);
    }
    return err;
  }

  public static OS3Exception getInternalError(Exception e) {
    return generateInternalError.apply(e);
  }
}
