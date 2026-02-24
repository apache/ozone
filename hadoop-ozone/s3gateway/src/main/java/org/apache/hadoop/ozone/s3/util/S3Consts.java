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

package org.apache.hadoop.ozone.s3.util;

import java.util.regex.Pattern;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;

/**
 * Set of constants used for S3 implementation.
 */
@InterfaceAudience.Private
public final class S3Consts {

  public static final String COPY_SOURCE_HEADER = "x-amz-copy-source";
  public static final String COPY_SOURCE_HEADER_RANGE =
      "x-amz-copy-source-range";
  public static final String STORAGE_CLASS_HEADER = "x-amz-storage-class";
  public static final String ENCODING_TYPE = "url";

  // Constants related to AWS Signature Version V4 calculation
  // https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
  public static final String X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";

  public static final String UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
  public static final String STREAMING_UNSIGNED_PAYLOAD_TRAILER = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
  public static final String STREAMING_AWS4_HMAC_SHA256_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
  public static final String STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER =
      "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
  public static final String STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD = "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD";
  public static final String STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER =
      "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER";

  public static final String AWS_CHUNKED = "aws-chunked";
  public static final String MULTI_CHUNKS_UPLOAD_PREFIX = "STREAMING";

  // Constants related to Range Header
  public static final String COPY_SOURCE_IF_PREFIX = "x-amz-copy-source-if-";
  public static final String COPY_SOURCE_IF_MODIFIED_SINCE =
      COPY_SOURCE_IF_PREFIX + "modified-since";
  public static final String COPY_SOURCE_IF_UNMODIFIED_SINCE =
      COPY_SOURCE_IF_PREFIX + "unmodified-since";

  // Constants related to Range Header
  public static final String RANGE_HEADER_SUPPORTED_UNIT = "bytes";
  public static final String RANGE_HEADER = "Range";
  public static final String ACCEPT_RANGE_HEADER = "Accept-Ranges";
  public static final String CONTENT_RANGE_HEADER = "Content-Range";

  public static final Pattern RANGE_HEADER_MATCH_PATTERN =
      Pattern.compile("bytes=(?<start>[0-9]*)-(?<end>[0-9]*)");

  //Error code 416 is Range Not Satisfiable
  public static final int RANGE_NOT_SATISFIABLE = 416;

  public static final String S3_XML_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/";

  // Constants related to custom metadata
  public static final String CUSTOM_METADATA_HEADER_PREFIX = "x-amz-meta-";
  public static final String CUSTOM_METADATA_COPY_DIRECTIVE_HEADER = "x-amz-metadata-directive";
  public static final String STORAGE_CONFIG_HEADER = "storage-config";

  public static final String DECODED_CONTENT_LENGTH_HEADER =
      "x-amz-decoded-content-length";

  // Constants related to S3 tags
  public static final String TAG_HEADER = "x-amz-tagging";
  public static final String TAG_DIRECTIVE_HEADER = "x-amz-tagging-directive";
  public static final String TAG_COUNT_HEADER = "x-amz-tagging-count";
  public static final String AWS_TAG_PREFIX = "aws:";

  public static final int TAG_NUM_LIMIT = 10;
  public static final int TAG_KEY_LENGTH_LIMIT = 128;
  public static final int TAG_VALUE_LENGTH_LIMIT = 256;
  // See https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_S3Tag.html
  // Also see https://docs.aws.amazon.com/directoryservice/latest/devguide/API_Tag.html for Java regex equivalent
  public static final Pattern TAG_REGEX_PATTERN = Pattern.compile("^([\\p{L}\\p{Z}\\p{N}_.:/=+\\-]*)$");
  public static final String MP_PARTS_COUNT = "x-amz-mp-parts-count";

  // Bucket owner condition headers
  // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html
  public static final String EXPECTED_BUCKET_OWNER_HEADER = "x-amz-expected-bucket-owner";
  public static final String EXPECTED_SOURCE_BUCKET_OWNER_HEADER = "x-amz-source-expected-bucket-owner";

  public static final String CHECKSUM_HEADER = "Content-MD5";

  // Conditional request headers
  public static final String IF_NONE_MATCH_HEADER = "If-None-Match";
  public static final String IF_MATCH_HEADER = "If-Match";

  //Never Constructed
  private S3Consts() {

  }

  /**
   * Copy directive for metadata and tags.
   */
  public enum CopyDirective {
    COPY, // Default directive
    REPLACE
  }

  /** Constants for query parameters. */
  public static final class QueryParams {
    public static final String ACL = "acl";
    public static final String CONTINUATION_TOKEN = "continuation-token";
    public static final String DELETE = "delete";
    public static final String DELIMITER = "delimiter";
    public static final String ENCODING_TYPE = "encoding-type";
    public static final String KEY_MARKER = "key-marker";
    public static final String MARKER = "marker";
    public static final String MAX_KEYS = "max-keys";
    public static final String MAX_PARTS = "max-parts";
    public static final String MAX_UPLOADS = "max-uploads";
    public static final String PART_NUMBER = "partNumber";
    public static final String PART_NUMBER_MARKER = "part-number-marker";
    public static final String PREFIX = "prefix";
    public static final String START_AFTER = "start-after";
    public static final String TAGGING = "tagging";
    public static final String UPLOAD_ID = "uploadId";
    public static final String UPLOAD_ID_MARKER = "upload-id-marker";
    public static final String UPLOADS = "uploads";

    private QueryParams() {
      // no instances
    }
  }

}
