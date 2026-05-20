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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Input stream implementation to read body of a signed chunked upload. This should also work
 * with the chunked payloads with trailer.
 *
 * <p>
 * Example chunk data:
 * <pre>
 * 10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n
 * &lt;65536-bytes&gt;\r\n
 * 400;chunk-signature=1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7\r\n
 * &lt;1024-bytes&gt;\r\n
 * 0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n
 * x-amz-checksum-crc32c:sOO8/Q==\r\n
 * x-amz-trailer-signature:63bddb248ad2590c92712055f51b8e78ab024eead08276b24f010b0efd74843f\r\n
 * </pre>
 * </p>
 * For the first chunk 10000 will be read and decoded from base-16 representation to 65536, which is the size of
 * the first chunk payload. Each chunk upload ends with a zero-byte final additional chunk.
 * At the end, there might be a trailer checksum payload and signature, depending on whether the x-amz-content-sha256
 * header value contains "-TRAILER" suffix (e.g. STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
 * and STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER) and "x-amz-trailer" is specified (e.g. x-amz-checksum-crc32c).
 * <p>
 *
 * <p>
 * The logic is similar to {@link UnsignedChunksInputStream}, but there is a "chunk-signature" to parse.
 * </p>
 *
 * <p>
 * Note that there are no actual chunk signature verification taking place. The InputStream only
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
public class SignedChunksInputStream extends InputStream {

  private final Pattern signatureLinePattern =
      Pattern.compile("([0-9A-Fa-f]+);chunk-signature=.*");

  private final InputStream originalStream;

  /**
   * Size of the chunk payload. If zero, the signature line should be parsed to
   * retrieve the subsequent chunk payload size.
   */
  private int remainingData = 0;

  /**
   * Every chunked uploads (multiple chunks) contains an additional final zero-byte
   * chunk. This can be used as the end-of-file marker.
   */
  private boolean isFinalChunkEncountered = false;

  public SignedChunksInputStream(InputStream inputStream) {
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
        // there is always a final zero byte chunk so we can stop reading
        // if we encounter this chunk
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
    //  10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n
    //  <65536-bytes>\r\n
    //
    // 10000 will be read and decoded from base-16 representation to 65536, which is the size of
    // the subsequent chunk payload.
    String signatureLine = buf.toString().trim();
    if (signatureLine.isEmpty()) {
      return -1;
    }

    //parse the data length.
    Matcher matcher = signatureLinePattern.matcher(signatureLine);
    if (matcher.matches()) {
      return Integer.parseInt(matcher.group(1), 16);
    } else {
      throw new IOException("Invalid signature line: " + signatureLine);
    }
  }
}
