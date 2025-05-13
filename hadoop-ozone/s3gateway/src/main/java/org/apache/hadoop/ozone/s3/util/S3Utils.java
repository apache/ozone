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
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

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
      if (S3StorageType.STANDARD_IA.name().equals(s3StorageType)) {
        return (!StringUtils.isEmpty(s3StorageConfig)) ? new ECReplicationConfig(s3StorageConfig) :
            new ECReplicationConfig(S3StorageType.STANDARD_IA.getEcReplicationString());
      } else {
        S3StorageType storageType = S3StorageType.valueOf(s3StorageType);
        return ReplicationConfig.fromProtoTypeAndFactor(
            ReplicationType.toProto(storageType.getType()),
            ReplicationFactor.toProto(storageType.getFactor()));
      }
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

  public static boolean hasSignedPayloadHeader(HttpHeaders headers) {
    final String signingAlgorithm = headers.getHeaderString(X_AMZ_CONTENT_SHA256);
    if (signingAlgorithm == null) {
      return false;
    }

    // Handles both AWS Signature Version 4 (HMAC-256) and AWS Signature Version 4A (ECDSA-P256-SHA256)
    return signingAlgorithm.equals(STREAMING_AWS4_HMAC_SHA256_PAYLOAD) ||
        signingAlgorithm.equals(STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER) ||
        signingAlgorithm.equals(STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD) ||
        signingAlgorithm.equals(STREAMING_AWS4_ECDSA_P256_SHA256_PAYLOAD_TRAILER);
  }
}
