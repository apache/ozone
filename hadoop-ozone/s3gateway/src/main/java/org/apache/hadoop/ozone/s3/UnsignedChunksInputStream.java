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

import static org.apache.hadoop.ozone.s3.util.S3Utils.eol;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Input stream implementation to read body of an unsigned chunked upload.
 * <p>
 * Currently, the only valid value of x-amz-content-sha256 header to indicate
 * transfer unsigned payload in multiple chunks is STREAMING-UNSIGNED-PAYLOAD-TRAILER.
 * Therefore, the input stream should work with chunked payloads with checksum trailer.
 * Nevertheless, this input stream also supports chunked upload without trailer.
 * </p>
 * <p>
 * Example chunk data:
 * <pre>
 * 10000\r\n
 * &lt;65536-bytes&gt;\r\n
 * 0\r\n
 * x-amz-checksum-crc64nvme:2wstOANdZ/o=\r\n
 * </pre>
 * </p>
 * <p>
 * The 10000 will be read and decoded from base-16 representation to 65536, which is the size of
 * the subsequent chunk payload. Each chunk upload ends with a zero-byte final additional chunk.
 * At the end, there will be a trailer checksum payload
 * </p>
 *
 * <p>
 * The logic is similar to {@link SignedChunksInputStream}, but since it is an unsigned chunked upload
 * there is no "chunk-signature" to parse.
 * </p>
 *
 * <p>
 * Note that there is not actual trailer checksum verification taking place. The InputStream only
 * returns the actual chunk payload from chunked signatures format.
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
public class UnsignedChunksInputStream extends InputStream {

  private final InputStream originalStream;

  /**
   * Size of the chunk payload. If zero, the content length should be parsed to
   * retrieve the subsequent chunk payload size.
   */
  private int remainingData = 0;

  /**
   * Every chunked uploads (multiple chunks) contains an additional final zero-byte
   * chunk. This can be used as the end-of-file marker.
   */
  private boolean isFinalChunkEncountered = false;

  public UnsignedChunksInputStream(InputStream inputStream) {
    originalStream = inputStream;
  }

  @Override
  public int read() throws IOException {
    if (isFinalChunkEncountered) {
      return -1;
    }
    if (remainingData > 0) {
      int curr = originalStream.read();
      remainingData--;
      if (remainingData == 0) {
        //read the "\r\n" at the end of the data section
        originalStream.read();
        originalStream.read();
      }
      return curr;
    } else {
      remainingData = readContentLengthFromHeader();
      if (remainingData <= 0) {
        // since currently trailer checksum verification is not supported, we can
        // stop reading after encountering the final zero-byte chunk.
        isFinalChunkEncountered = true;
        return -1;
      }
      return read();
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "b == null");
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException("Offset=" + off + " and len="
          + len + " don't match the array length of " + b.length);
    } else if (len == 0) {
      return 0;
    } else if (isFinalChunkEncountered) {
      return -1;
    }
    int currentOff = off;
    int currentLen = len;
    int totalReadBytes = 0;
    int realReadLen = 0;
    int maxReadLen = 0;
    do {
      if (remainingData > 0) {
        // The chunk payload size has been decoded, now read the actual chunk payload
        maxReadLen = Math.min(remainingData, currentLen);
        realReadLen = originalStream.read(b, currentOff, maxReadLen);
        if (realReadLen == -1) {
          break;
        }
        currentOff += realReadLen;
        currentLen -= realReadLen;
        totalReadBytes += realReadLen;
        remainingData -= realReadLen;
        if (remainingData == 0) {
          //read the "\r\n" at the end of the data section
          originalStream.read();
          originalStream.read();
        }
      } else {
        remainingData = readContentLengthFromHeader();
        if (remainingData == 0) {
          // there is always a final zero byte chunk so we can stop reading
          // if we encounter this chunk
          isFinalChunkEncountered = true;
        }
        if (isFinalChunkEncountered || remainingData == -1) {
          break;
        }
      }
    } while (currentLen > 0);
    return totalReadBytes > 0 ? totalReadBytes : -1;
  }

  private int readContentLengthFromHeader() throws IOException {
    int prev = -1;
    int curr = 0;
    StringBuilder buf = new StringBuilder();

    //read everything until the next \r\n
    while (!eol(prev, curr) && curr != -1) {
      int next = originalStream.read();
      if (next != -1) {
        buf.append((char) next);
      }
      prev = curr;
      curr = next;
    }
    // Example of a single chunk data:
    //  10000\r\n
    //  <65536-bytes>\r\n
    //
    // 10000 will be read and decoded from base-16 representation to 65536, which is the size of
    // the subsequent chunk payload.
    String readString = buf.toString().trim();
    if (readString.isEmpty()) {
      return -1;
    }
    return Integer.parseInt(readString, 16);
  }
}
