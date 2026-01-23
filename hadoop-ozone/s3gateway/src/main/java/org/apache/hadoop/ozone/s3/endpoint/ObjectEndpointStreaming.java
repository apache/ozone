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

import static org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateSignatureHeader;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.KeyMetadataAware;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key level rest endpoints for Streaming.
 */
final class ObjectEndpointStreaming {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpointStreaming.class);
  private static final S3GatewayMetrics METRICS = S3GatewayMetrics.getMetrics();

  private ObjectEndpointStreaming() {
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static Pair<String, Long> put(
      OzoneBucket bucket, String keyPath,
      long length, ReplicationConfig replicationConfig,
      int chunkSize, Map<String, String> keyMetadata,
      Map<String, String> tags, MultiDigestInputStream body,
      HttpHeaders headers, boolean isSignedPayload,
      PerformanceStringBuilder perf)
      throws IOException, OS3Exception {

    try {
      return putKeyWithStream(bucket, keyPath,
          length, chunkSize, replicationConfig, keyMetadata, tags, body, headers, isSignedPayload, perf);
    } catch (IOException ex) {
      LOG.error("Exception occurred in PutObject", ex);
      if (ex instanceof OMException) {
        if (((OMException) ex).getResult() ==
            OMException.ResultCodes.NOT_A_FILE) {
          OS3Exception os3Exception = S3ErrorTable.newError(INVALID_REQUEST,
              keyPath);
          os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
              "when calling the PutObject/MPU PartUpload operation: " +
              OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
              " considered as Unix Paths. Path has Violated FS Semantics " +
              "which caused put operation to fail.");
          throw os3Exception;
        } else if ((((OMException) ex).getResult() ==
            OMException.ResultCodes.PERMISSION_DENIED)) {
          throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, keyPath);
        }
      }
      throw ex;
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static Pair<String, Long> putKeyWithStream(
      OzoneBucket bucket,
      String keyPath,
      long length,
      int bufferSize,
      ReplicationConfig replicationConfig,
      Map<String, String> keyMetadata,
      Map<String, String> tags,
      MultiDigestInputStream body,
      HttpHeaders headers,
      boolean isSignedPayload,
      PerformanceStringBuilder perf)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    final String amzContentSha256Header = validateSignatureHeader(headers, keyPath, isSignedPayload);
    long writeLen;
    String md5Hash;
    try (OzoneDataStreamOutput streamOutput = bucket.createStreamKey(keyPath,
        length, replicationConfig, keyMetadata, tags)) {
      long metadataLatencyNs = METRICS.updatePutKeyMetadataStats(startNanos);
      writeLen = writeToStreamOutput(streamOutput, body, bufferSize, length);
      md5Hash = DatatypeConverter.printHexBinary(body.getMessageDigest(OzoneConsts.MD5_HASH).digest())
          .toLowerCase();
      perf.appendMetaLatencyNanos(metadataLatencyNs);
      ((KeyMetadataAware)streamOutput).getMetadata().put(OzoneConsts.ETAG, md5Hash);

      List<CheckedRunnable<IOException>> preCommits = new ArrayList<>();
      String clientContentMD5 = headers.getHeaderString(S3Consts.CHECKSUM_HEADER);
      if (clientContentMD5 != null) {
        CheckedRunnable<IOException> checkContentMD5Hook = () -> {
          S3Utils.validateContentMD5(clientContentMD5, md5Hash, keyPath);
        };
        preCommits.add(checkContentMD5Hook);
      }

      // If sha256Digest exists, this request must validate x-amz-content-sha256
      MessageDigest sha256Digest = body.getMessageDigest(OzoneConsts.FILE_HASH);
      if (sha256Digest != null) {
        final String actualSha256 = DatatypeConverter.printHexBinary(
            sha256Digest.digest()).toLowerCase();
        CheckedRunnable<IOException> checkSha256Hook = () -> {
          if (!amzContentSha256Header.equals(actualSha256)) {
            throw S3ErrorTable.newError(S3ErrorTable.X_AMZ_CONTENT_SHA256_MISMATCH, keyPath);
          }
        };
        preCommits.add(checkSha256Hook);
      }

      streamOutput.getKeyDataStreamOutput().setPreCommits(preCommits);
    }
    return Pair.of(md5Hash, writeLen);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static long copyKeyWithStream(
      OzoneBucket bucket,
      String keyPath,
      long length,
      int bufferSize,
      ReplicationConfig replicationConfig,
      Map<String, String> keyMetadata,
      DigestInputStream body, PerformanceStringBuilder perf, long startNanos,
      Map<String, String> tags)
      throws IOException {
    long writeLen;
    try (OzoneDataStreamOutput streamOutput = bucket.createStreamKey(keyPath,
        length, replicationConfig, keyMetadata, tags)) {
      long metadataLatencyNs =
          METRICS.updateCopyKeyMetadataStats(startNanos);
      writeLen = writeToStreamOutput(streamOutput, body, bufferSize, length);
      String eTag = DatatypeConverter.printHexBinary(body.getMessageDigest().digest())
          .toLowerCase();
      perf.appendMetaLatencyNanos(metadataLatencyNs);
      ((KeyMetadataAware)streamOutput).getMetadata().put(OzoneConsts.ETAG, eTag);
    }
    return writeLen;
  }

  private static long writeToStreamOutput(OzoneDataStreamOutput streamOutput,
                                          InputStream body, int bufferSize,
                                          long length)
      throws IOException {
    final byte[] buffer = new byte[bufferSize];
    long n = 0;
    while (n < length) {
      final int toRead = Math.toIntExact(Math.min(bufferSize, length - n));
      final int readLength = body.read(buffer, 0, toRead);
      if (readLength == -1) {
        break;
      }
      streamOutput.write(ByteBuffer.wrap(buffer, 0, readLength));
      n += readLength;
    }
    return n;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public static Response createMultipartKey(OzoneBucket ozoneBucket, String key,
      long length, int partNumber, String uploadID, int chunkSize,
      MultiDigestInputStream body, PerformanceStringBuilder perf, HttpHeaders headers)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    String eTag;
    try {
      try (OzoneDataStreamOutput streamOutput = ozoneBucket
          .createMultipartStreamKey(key, length, partNumber, uploadID)) {
        long metadataLatencyNs = METRICS.updatePutKeyMetadataStats(startNanos);
        long putLength =
            writeToStreamOutput(streamOutput, body, chunkSize, length);
        eTag = DatatypeConverter.printHexBinary(
            body.getMessageDigest(OzoneConsts.MD5_HASH).digest()).toLowerCase();
        String clientContentMD5 = headers.getHeaderString(S3Consts.CHECKSUM_HEADER);
        if (clientContentMD5 != null) {
          CheckedRunnable<IOException> checkContentMD5Hook = () -> {
            S3Utils.validateContentMD5(clientContentMD5, eTag, key);
          };
          streamOutput.getKeyDataStreamOutput().setPreCommits(Collections.singletonList(checkContentMD5Hook));
        }
        ((KeyMetadataAware)streamOutput).getMetadata().put(OzoneConsts.ETAG, eTag);
        METRICS.incPutKeySuccessLength(putLength);
        perf.appendMetaLatencyNanos(metadataLatencyNs);
        perf.appendSizeBytes(putLength);
      }
    } catch (OMException ex) {
      if (ex.getResult() ==
          OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw S3ErrorTable.newError(NO_SUCH_UPLOAD,
            uploadID);
      } else if (ex.getResult() == OMException.ResultCodes.PERMISSION_DENIED) {
        throw S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED,
            ozoneBucket.getName() + "/" + key);
      }
      throw ex;
    }
    return Response.ok()
        .header(HttpHeaders.ETAG, wrapInQuotes(eTag))
        .build();
  }
}
