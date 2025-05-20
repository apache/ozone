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
package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import javax.annotation.Nonnull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.AWS_CHUNKED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MULTI_CHUNKS_UPLOAD_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_UNSIGNED_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;

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
   * @param clientConfiguredReplConfig - Client side configured replication
   *                                   config.
   * @param bucketReplConfig           - server side bucket default replication
   *                                   config.
   * @return client resolved replication config.
   */
  public static ReplicationConfig resolveS3ClientSideReplicationConfig(
      String s3StorageTypeHeader, ReplicationConfig clientConfiguredReplConfig,
      ReplicationConfig bucketReplConfig)
      throws OS3Exception {
    ReplicationConfig clientDeterminedReplConfig = null;

    // Let's map the user provided s3 storage type header to ozone s3 storage
    // type.
    S3StorageType s3StorageType = null;
    if (s3StorageTypeHeader != null && !s3StorageTypeHeader.equals("")) {
      s3StorageType = toS3StorageType(s3StorageTypeHeader);
    }

    boolean isECBucket = bucketReplConfig != null && bucketReplConfig
        .getReplicationType() == HddsProtos.ReplicationType.EC;

    // if bucket replication config configured with EC, we will give high
    // preference to server side bucket defaults.
    // Why we give high preference to EC is, there is no way for file system
    // interfaces to pass EC replication. So, if one configures EC at bucket,
    // we consider EC to take preference. in short, keys created from file
    // system under EC bucket will always be EC'd.
    if (isECBucket) {
      // if bucket is EC, don't bother client provided configs, let's pass
      // bucket config.
      clientDeterminedReplConfig = bucketReplConfig;
    } else {
      // Let's validate the client side available replication configs.
      boolean isUserPassedReplicationInSupportedList =
          s3StorageType != null && (s3StorageType.getFactor()
              .getValue() == ReplicationFactor.ONE.getValue() || s3StorageType
              .getFactor().getValue() == ReplicationFactor.THREE.getValue());
      if (isUserPassedReplicationInSupportedList) {
        clientDeterminedReplConfig = ReplicationConfig.fromProtoTypeAndFactor(
            ReplicationType.toProto(s3StorageType.getType()),
            ReplicationFactor.toProto(s3StorageType.getFactor()));
      } else {
        // API passed replication number is not in supported replication list.
        // So, let's use whatever available in client side configured.
        // By default it will be null, so server will use server defaults.
        clientDeterminedReplConfig = clientConfiguredReplConfig;
      }
    }
    return clientDeterminedReplConfig;
  }

  public static S3StorageType toS3StorageType(String storageType)
      throws OS3Exception {
    try {
      return S3StorageType.valueOf(storageType);
    } catch (IllegalArgumentException ex) {
      throw newError(INVALID_ARGUMENT, storageType, ex);
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

  public static String validateSignatureHeader(HttpHeaders headers, String resource) throws OS3Exception {
    String xAmzContentSha256Header = headers.getHeaderString(X_AMZ_CONTENT_SHA256);
    if (xAmzContentSha256Header == null) {
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
}
