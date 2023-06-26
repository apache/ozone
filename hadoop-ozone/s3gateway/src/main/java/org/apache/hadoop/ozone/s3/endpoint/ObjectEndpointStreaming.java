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

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;

/**
 * Key level rest endpoints for Streaming.
 */
final class ObjectEndpointStreaming {

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpointStreaming.class);

  private ObjectEndpointStreaming() {
  }

  public static long put(OzoneBucket bucket, String keyPath,
                         long length, ReplicationConfig replicationConfig,
                         int chunkSize, Map<String, String> keyMetadata,
                         InputStream body)
      throws IOException, OS3Exception {

    try {
      return putKeyWithStream(bucket, keyPath,
          length, chunkSize, replicationConfig, keyMetadata, body);
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

  public static long putKeyWithStream(OzoneBucket bucket,
                                      String keyPath,
                                      long length,
                                      int bufferSize,
                                      ReplicationConfig replicationConfig,
                                      Map<String, String> keyMetadata,
                                      InputStream body)
      throws IOException {
    long writeLen = 0;
    try (OzoneDataStreamOutput streamOutput = bucket.createStreamKey(keyPath,
        length, replicationConfig, keyMetadata)) {
      writeLen = writeToStreamOutput(streamOutput, body, bufferSize, length);
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
}
