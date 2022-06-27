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

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.RangeHeaderParserUtil;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;

/**
 * Key level rest endpoints for Streaming.
 */
final class ObjectEndpointStreaming {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpointStreaming.class);

  private ObjectEndpointStreaming() {
  }

  public static Response put(OzoneBucket bucket, String keyPath,
                             long length, ReplicationConfig replicationConfig,
                             int chunkSize, InputStream body)
      throws IOException, OS3Exception {

    try {
      Map<String, String> keyMetadata = new HashMap<>();
      putKeyWithStream(bucket, keyPath,
          length, chunkSize, replicationConfig, keyMetadata, body);
      return Response.ok().status(HttpStatus.SC_OK).build();
    } catch (IOException ex) {
      LOG.error("Exception occurred in PutObject", ex);
      if (ex instanceof OMException) {
        if (((OMException) ex).getResult() ==
            OMException.ResultCodes.NOT_A_FILE) {
          OS3Exception os3Exception = S3ErrorTable.newError(INVALID_REQUEST,
              keyPath);
          os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
              "when calling the PutObject/MPU PartUpload operation: " +
              OZONE_OM_ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
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

  public static void putKeyWithStream(OzoneBucket bucket,
                                      String keyPath,
                                      long length,
                                      int bufferSize,
                                      ReplicationConfig replicationConfig,
                                      Map<String, String> keyMetadata,
                                      InputStream body)
      throws IOException {
    try (OzoneDataStreamOutput streamOutput = bucket.createStreamKey(keyPath,
        length, replicationConfig, keyMetadata)) {
      writeToStreamOutput(streamOutput, body, bufferSize, length);
    }
  }

  private static void writeToStreamOutput(OzoneDataStreamOutput streamOutput,
                                          InputStream body, int bufferSize)
      throws IOException {
    writeToStreamOutput(streamOutput, body, bufferSize, Long.MAX_VALUE);
  }

  private static void writeToStreamOutput(OzoneDataStreamOutput streamOutput,
                                          InputStream body, int bufferSize,
                                          long length)
      throws IOException {
    byte[] buffer = new byte[bufferSize];
    ByteBuffer writeByteBuffer;
    long total = 0;
    do {
      int realBufferSize = (int) (length - total);
      if (realBufferSize > 0 && realBufferSize < bufferSize) {
        buffer = new byte[realBufferSize];
      }
      int nn = body.read(buffer);
      if (nn == -1) {
        break;
      } else if (nn != bufferSize) {
        byte[] subBuffer = new byte[nn];
        System.arraycopy(buffer, 0, subBuffer, 0, nn);
        writeByteBuffer = ByteBuffer.wrap(subBuffer, 0, nn);
      } else {
        writeByteBuffer = ByteBuffer.wrap(buffer, 0, nn);
      }
      streamOutput.write(writeByteBuffer, 0, nn);
      total += nn;
    } while (total != length);
  }


  public static Response createMultipartKey(OzoneBucket ozoneBucket, String key,
                                            long length, int partNumber,
                                            String uploadID, int chunkSize,
                                            InputStream body)
      throws IOException, OS3Exception {
    try {
      OzoneDataStreamOutput streamOutput = null;
      try (OzoneDataStreamOutput ozoneStreamOutput = ozoneBucket
          .createMultipartStreamKey(
              key, length, partNumber, uploadID)) {
        writeToStreamOutput(ozoneStreamOutput, body, chunkSize);
        streamOutput = ozoneStreamOutput;
      }

      String eTag = "";
      if (streamOutput != null) {
        OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
            streamOutput.getCommitUploadPartInfo();
        eTag = omMultipartCommitUploadPartInfo.getPartName();
      }

      return Response.ok().header("ETag",
          eTag).build();
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
  }

  public static Response copyMultipartKey(Pair<OzoneBucket, String> source,
                                          Pair<OzoneBucket, String> target,
                                          long length, int partNumber,
                                          String uploadID, int chunkSize,
                                          HttpHeaders headers)
      throws IOException, OS3Exception {

    OzoneBucket sourceBucket = source.getLeft();
    OzoneBucket ozoneBucket = target.getLeft();
    String sourceKey = source.getRight();
    String key = target.getRight();

    try {
      OzoneDataStreamOutput ozoneStreamOutput = ozoneBucket
          .createMultipartStreamKey(key, length, partNumber, uploadID);

      Long sourceKeyModificationTime = sourceBucket.
          getKey(sourceKey).getModificationTime().toEpochMilli();
      String copySourceIfModifiedSince =
          headers.getHeaderString(COPY_SOURCE_IF_MODIFIED_SINCE);
      String copySourceIfUnmodifiedSince =
          headers.getHeaderString(COPY_SOURCE_IF_UNMODIFIED_SINCE);
      if (!ObjectEndpoint
          .checkCopySourceModificationTime(sourceKeyModificationTime,
              copySourceIfModifiedSince, copySourceIfUnmodifiedSince)) {
        throw S3ErrorTable.newError(PRECOND_FAILED,
            sourceBucket + "/" + sourceKey);
      }

      try (OzoneInputStream sourceObject =
               sourceBucket.readKey(sourceKey)) {

        String range =
            headers.getHeaderString(COPY_SOURCE_HEADER_RANGE);
        if (range != null) {
          RangeHeader rangeHeader =
              RangeHeaderParserUtil.parseRangeHeader(range, 0);
          LOG.info("Copy range {} after parse {}", range, rangeHeader);
          final long skipped =
              sourceObject.skip(rangeHeader.getStartOffset());
          if (skipped != rangeHeader.getStartOffset()) {
            throw new EOFException(
                "Bytes to skip: "
                    + rangeHeader.getStartOffset() + " actual: " + skipped);
          }
          writeToStreamOutput(ozoneStreamOutput, sourceObject, chunkSize,
              rangeHeader.getEndOffset() - rangeHeader.getStartOffset() +
                  1);
        } else {
          writeToStreamOutput(ozoneStreamOutput, sourceObject, chunkSize);
        }
      }

      String eTag = "";
      if (ozoneStreamOutput != null) {
        ozoneStreamOutput.close();
        OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
            ozoneStreamOutput.getCommitUploadPartInfo();
        eTag = omMultipartCommitUploadPartInfo.getPartName();
      }

      return Response.ok(new CopyPartResult(eTag)).build();
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
  }

}
