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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_STORAGE_CLASS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.AWS_CHUNKED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MULTI_CHUNKS_UPLOAD_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_UNSIGNED_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;

import jakarta.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Objects;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Utilities.
 */
public final class S3Utils {

  public static String urlDecode(String str)
      throws UnsupportedEncodingException {
    return URLDecoder.decode(str, UTF_8.name());
  }

  public static String urlEncode(String str)
      throws UnsupportedEncodingException {
    return URLEncoder.encode(str, UTF_8.name());
  }

  private S3Utils() {
    // no instances
  }

  /**
   * This API used to resolve the client side configuration preference for file
   * system layer implementations.
   *
   * @param s3StorageTypeHeader        - s3 user passed storage type
   *                                   header.
   * @param bucketReplConfig           - server side bucket default replication
   *                                   config.
   * @param clientConfiguredReplConfig - Client side configured replication
   *                                   config.
   * @return client resolved replication config.
   */
  public static ReplicationConfig resolveS3ClientSideReplicationConfig(
      String s3StorageTypeHeader, String s3StorageConfigHeader,
      ReplicationConfig clientConfiguredReplConfig,
      ReplicationConfig bucketReplConfig)
      throws OS3Exception {

    // If user provided s3 storage type header is not null then map it
    // to ozone replication config
    if (!StringUtils.isEmpty(s3StorageTypeHeader)) {
      return toReplicationConfig(s3StorageTypeHeader, s3StorageConfigHeader);
    }

    // If client configured replication config is null then default to bucket replication
    // otherwise default to server side default replication config.
    return (clientConfiguredReplConfig != null) ?
        clientConfiguredReplConfig : bucketReplConfig;
  }

  public static ReplicationConfig toReplicationConfig(String s3StorageType, String s3StorageConfig)
      throws OS3Exception {
    try {
      S3StorageType storageType = S3StorageType.valueOf(s3StorageType);
      if (S3StorageType.STANDARD_IA.equals(storageType) &&
          !StringUtils.isEmpty(s3StorageConfig)) {
        return new ECReplicationConfig(s3StorageConfig);
      }
      return storageType.getReplicationConfig();
    } catch (IllegalArgumentException ex) {
      throw newError(INVALID_STORAGE_CLASS, s3StorageType, ex);
    }
  }

  public static WebApplicationException wrapOS3Exception(OS3Exception ex) {
    return new WebApplicationException(ex.getErrorMessage(), ex,
        Response.status(ex.getHttpCode())
            .entity(ex.toXml())
            .build());
  }

  public static boolean hasUnsignedPayload(@Nonnull String amzContentSha256Header) {
    Objects.requireNonNull(amzContentSha256Header);
    return amzContentSha256Header.equals(UNSIGNED_PAYLOAD) ||
        amzContentSha256Header.equals(STREAMING_UNSIGNED_PAYLOAD_TRAILER);
  }

  public static boolean hasMultiChunksPayload(@Nonnull String amzContentSha256Header) {
    Objects.requireNonNull(amzContentSha256Header);
    // Multiple chunk uploads
    // - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
    // - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
    // Possible values
    // - STREAMING-UNSIGNED-PAYLOAD-TRAILER
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD
    // - STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
    // - STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD
    // - STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER
    // Currently since all the multi chunks values have x-amz-content-sha256 header value that starts
    // with STREAMING, we can use this prefix to differentiates between multi chunks and single chunks upload.
    // In the future if there are more multi chunks signature algorithms that has the same prefix,
    // this function will be able to handle detect it.
    return amzContentSha256Header.startsWith(MULTI_CHUNKS_UPLOAD_PREFIX);
  }

  public static void validateMultiChunksUpload(HttpHeaders headers, String amzDecodedContentLength,
                                               String resource) throws OS3Exception {
    final String contentEncoding = headers.getHeaderString(HttpHeaders.CONTENT_ENCODING);
    // "Content-Encoding : aws-chunked" seems to only be sent for SDK V2, so ignore if there is no
    // Content-Encoding header
    if (contentEncoding != null) {
      // Amazon S3 supports multiple content encoding values for example "Content-Encoding : aws-chunked,gzip"
      // We are only interested on "aws-chunked"
      boolean containsAwsChunked = Arrays.stream(contentEncoding.split(","))
          .map(String::trim)
          .anyMatch(AWS_CHUNKED::equals);
      if (!containsAwsChunked) {
        OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, resource);
        ex.setErrorMessage("An error occurred (InvalidArgument) for multi chunks upload: " +
            "The " + HttpHeaders.CONTENT_ENCODING + " header does not contain " + AWS_CHUNKED);
        throw ex;
      }
    }

    if (amzDecodedContentLength == null) {
      OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, resource);
      ex.setErrorMessage("An error occurred (InvalidArgument) for multi chunks upload: " +
          "The " + DECODED_CONTENT_LENGTH_HEADER + " header is not specified");
      throw ex;
    }
  }

  public static String validateSignatureHeader(HttpHeaders headers, String resource, boolean isSignedPayload)
      throws OS3Exception {
    String xAmzContentSha256Header = headers.getHeaderString(X_AMZ_CONTENT_SHA256);
    if (xAmzContentSha256Header == null) {
      if (!isSignedPayload) {
        return UNSIGNED_PAYLOAD;
      }
      OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, resource);
      ex.setErrorMessage("An error occurred (InvalidArgument): " +
          "The " + X_AMZ_CONTENT_SHA256 + " header is not specified");
      throw ex;
    }

    return xAmzContentSha256Header;
  }

  /**
   * Checks if the given pair of bytes represent the end-of-line sequence (\r\n).
   *
   * @param prev the previous byte value (should be 13 for '\r')
   * @param curr the current byte value (should be 10 for '\n')
   * @return true if the pair forms a CRLF sequence, false otherwise
   */
  public static boolean eol(int prev, int curr) {
    return prev == 13 && curr == 10;
  }

  /**
   * Generates a Canonical User ID compatible with S3 by returning the
   * SHA-256 hexadecimal encoding of the input string.
   *
   * @param input the input string
   * @return the SHA-256 hexadecimal encoded Canonical User ID
   */
  public static String generateCanonicalUserId(String input) {
    return DigestUtils.sha256Hex(input);
  }

  /**
   * Strips leading and trailing double quotes from the given string.
   *
   * @param value the input string
   * @return the string without leading and trailing double quotes
   */
  public static String stripQuotes(String value) {
    return StringUtils.strip(value, "\"");
  }

  /**
   * Wraps the given string in double quotes.
   *
   * @param value the input string
   * @return the string wrapped in double quotes
   */
  public static String wrapInQuotes(String value) {
    return StringUtils.wrap(value, '\"');
  }

}
