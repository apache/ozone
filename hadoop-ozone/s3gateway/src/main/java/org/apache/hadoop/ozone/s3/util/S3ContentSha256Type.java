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

import java.util.Objects;

/** Classifies the x-amz-content-sha256 header without changing its signed value. */
public enum S3ContentSha256Type {
  UNSIGNED_PAYLOAD,
  STREAMING_UNSIGNED_PAYLOAD_TRAILER,
  STREAMING_AWS4_HMAC_SHA256_PAYLOAD,
  STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER,
  STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD,
  STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER,
  OTHER,
  OTHER_STREAMING;

  public static S3ContentSha256Type fromHeaderValue(String headerValue) {
    Objects.requireNonNull(headerValue);
    switch (headerValue) {
    case S3Consts.UNSIGNED_PAYLOAD:
      return UNSIGNED_PAYLOAD;
    case S3Consts.STREAMING_UNSIGNED_PAYLOAD_TRAILER:
      return STREAMING_UNSIGNED_PAYLOAD_TRAILER;
    case S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD:
      return STREAMING_AWS4_HMAC_SHA256_PAYLOAD;
    case S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER:
      return STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER;
    case S3Consts.STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD:
      return STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD;
    case S3Consts.STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER:
      return STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER;
    default:
      // Preserve streaming detection for unrecognized algorithms; other values are not validated here.
      return headerValue.startsWith(S3Consts.MULTI_CHUNKS_UPLOAD_PREFIX) ? OTHER_STREAMING : OTHER;
    }
  }

  public boolean isUnsigned() {
    return this == UNSIGNED_PAYLOAD || this == STREAMING_UNSIGNED_PAYLOAD_TRAILER;
  }

  public boolean isStreaming() {
    return this != UNSIGNED_PAYLOAD && this != OTHER;
  }

  public boolean isHmacSignedStreaming() {
    return this == STREAMING_AWS4_HMAC_SHA256_PAYLOAD || this == STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER;
  }
}
