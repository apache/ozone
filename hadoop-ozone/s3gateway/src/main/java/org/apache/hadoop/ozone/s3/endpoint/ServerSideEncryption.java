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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_ALGORITHM_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_KEY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_SSE_CUSTOMER_KEY_MD5_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CONTEXT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_ALGORITHM_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_CUSTOMER_KEY_MD5_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.SSE_KMS_KEY_ID_HEADER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Checks the server-side encryption (SSE) headers of S3 requests.
 * <p>
 * Ozone encrypts data at rest only in encrypted buckets, with the bucket's KMS key. A request for SSE-S3 or SSE-KMS
 * is honoured in an encrypted bucket, whose data is encrypted anyway, and rejected with {@code NotImplemented} in
 * any other bucket. SSE-C needs a customer-provided key, which Ozone cannot use, so it is always rejected. Either
 * way, a request that asks for encryption never stores unencrypted data with a success response.
 */
final class ServerSideEncryption {

  static final String AES256 = "AES256";
  static final String AWS_KMS = "aws:kms";
  static final String AWS_KMS_DSSE = "aws:kms:dsse";

  private static final List<String> CUSTOMER_KEY_HEADERS = ImmutableList.of(
      SSE_CUSTOMER_ALGORITHM_HEADER, SSE_CUSTOMER_KEY_HEADER, SSE_CUSTOMER_KEY_MD5_HEADER,
      COPY_SOURCE_SSE_CUSTOMER_ALGORITHM_HEADER, COPY_SOURCE_SSE_CUSTOMER_KEY_HEADER,
      COPY_SOURCE_SSE_CUSTOMER_KEY_MD5_HEADER);

  private ServerSideEncryption() {
  }

  /** Rejects a request with SSE-C headers, which are not supported. */
  static void rejectCustomerKey(HttpHeaders headers, String resource) {
    for (String header : CUSTOMER_KEY_HEADERS) {
      if (headers.getHeaderString(header) != null) {
        throw error(NOT_IMPLEMENTED, resource, "Server-side encryption with customer-provided keys is not supported");
      }
    }
  }

  /**
   * Checks the SSE headers of a request that writes to {@code bucket}.
   *
   * @return the SSE headers to return in the response, empty if the request does not ask for encryption
   */
  static Map<String, String> checkWrite(HttpHeaders headers, OzoneBucket bucket, String resource) {
    rejectCustomerKey(headers, resource);
    final String algorithm = headers.getHeaderString(SSE_HEADER);
    final String kmsKeyId = headers.getHeaderString(SSE_KMS_KEY_ID_HEADER);
    final String context = headers.getHeaderString(SSE_CONTEXT_HEADER);
    if (algorithm == null) {
      if (kmsKeyId != null || context != null) {
        throw error(INVALID_ARGUMENT, resource,
            "Server-side encryption with an AWS KMS key requires the header " + SSE_HEADER + ": " + AWS_KMS);
      }
      return ImmutableMap.of();
    }
    if (!AES256.equals(algorithm) && !AWS_KMS.equals(algorithm) && !AWS_KMS_DSSE.equals(algorithm)) {
      throw error(INVALID_ARGUMENT, resource, "The encryption method specified is not supported");
    }
    if (AES256.equals(algorithm) && (kmsKeyId != null || context != null)) {
      throw error(INVALID_ARGUMENT, resource,
          "A KMS key or encryption context requires the header " + SSE_HEADER + ": " + AWS_KMS);
    }

    final String bucketKey = bucket.getEncryptionKeyName();
    if (StringUtils.isEmpty(bucketKey)) {
      throw error(NOT_IMPLEMENTED, resource, "Server-side encryption is only supported in encrypted buckets");
    }
    if (AWS_KMS_DSSE.equals(algorithm) || context != null || (kmsKeyId != null && !kmsKeyId.equals(bucketKey))) {
      throw error(NOT_IMPLEMENTED, resource,
          "Only server-side encryption with the bucket's encryption key " + bucketKey + " is supported");
    }
    return AWS_KMS.equals(algorithm)
        ? ImmutableMap.of(SSE_HEADER, algorithm, SSE_KMS_KEY_ID_HEADER, bucketKey)
        : ImmutableMap.of(SSE_HEADER, algorithm);
  }

  /** Adds the SSE headers returned by {@link #checkWrite} to {@code response}. */
  static Response withHeaders(Response response, Map<String, String> sseHeaders) {
    if (sseHeaders.isEmpty()) {
      return response;
    }
    final Response.ResponseBuilder builder = Response.fromResponse(response);
    sseHeaders.forEach(builder::header);
    return builder.build();
  }

  private static OS3Exception error(S3ErrorTable code, String resource, String message) {
    final OS3Exception ex = newError(code, resource);
    ex.setErrorMessage(message);
    return ex;
  }
}
