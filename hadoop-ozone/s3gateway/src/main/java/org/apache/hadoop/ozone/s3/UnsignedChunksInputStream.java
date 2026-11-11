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

package org.apache.hadoop.ozone.s3;

import java.io.InputStream;
import java.util.regex.Pattern;

/**
 * Input stream implementation to read body of an unsigned chunked upload.
 * <p>
 * Currently, the only valid value of x-amz-content-sha256 header to indicate
 * transfer unsigned payload in multiple chunks is STREAMING-UNSIGNED-PAYLOAD-TRAILER.
 * Therefore, the input stream should work with chunked payloads with checksum trailer.
 * </p>
 * <p>
 * Example chunk data:
 * <pre>
 * 10000\r\n
 * &lt;65536-bytes&gt;\r\n
 * 0\r\n
 * x-amz-checksum-crc64nvme:2wstOANdZ/o=\r\n
 * \r\n
 * </pre>
 * </p>
 * <p>
 * The 10000 will be read and decoded from base-16 representation to 65536, which is the size of
 * the subsequent chunk payload. Each chunk upload ends with a zero-byte final additional chunk.
 * At the end, there will be a trailer checksum payload
 * </p>
 *
 * <p>
 * {@link AwsChunkedInputStream} does the parsing shared with {@link SignedChunksInputStream}. Since it is an
 * unsigned chunked upload, there is no "chunk-signature" to parse.
 * </p>
 *
 * <p>
 * This stream validates chunk and trailer framing, including the checksum encoding and length,
 * and compares the trailer checksum with the decoded payload before reporting completion.
 * </p>
 *
 * Reference:
 * <ul>
 *   <li>
 *     <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html">
 *        Signature Calculation: Transfer Payload in Multiple Chunks</a>
 *   </li>
 *   <li>
 *     <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html">
 *        Signature Calculation: Including Trailing Headers</a>
 *   </li>
 * </ul>
 */
public class UnsignedChunksInputStream extends AwsChunkedInputStream {

  private static final Pattern CHUNK_SIZE_PATTERN = Pattern.compile("[0-9A-Fa-f]{1,16}");

  public UnsignedChunksInputStream(InputStream inputStream, String keyPath, TrailerHeader trailerHeader) {
    super(inputStream, keyPath, trailerHeader);
    if (trailerHeader == TrailerHeader.NONE) {
      throw new IllegalArgumentException("Unsigned chunked uploads require a checksum trailer");
    }
  }

  @Override
  long parseChunkSize(String size) {
    if (!CHUNK_SIZE_PATTERN.matcher(size).matches()) {
      throw invalidBody("Invalid unsigned chunk size");
    }
    try {
      return Long.parseLong(size, 16);
    } catch (NumberFormatException ex) {
      throw invalidBody("Invalid unsigned chunk size");
    }
  }
}
