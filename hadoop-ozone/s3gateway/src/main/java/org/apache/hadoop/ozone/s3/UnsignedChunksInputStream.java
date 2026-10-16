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
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream.TrailerHeader;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

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
 * The logic is similar to {@link SignedChunksInputStream}, but since it is an unsigned chunked upload
 * there is no "chunk-signature" to parse.
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
public class UnsignedChunksInputStream extends InputStream {

  private static final Pattern CHUNK_SIZE_PATTERN = Pattern.compile("[0-9A-Fa-f]{1,16}");
  private static final int MAX_LINE_LENGTH = 8 * 1024;

  private final InputStream originalStream;
  private final String keyPath;
  private final TrailerHeader trailerHeader;
  private final TrailerChecksum payloadChecksum;
  private long remainingData;
  private boolean isFinalChunkEncountered;
  private byte[] trailerChecksum;

  public UnsignedChunksInputStream(InputStream inputStream, String keyPath, TrailerHeader trailerHeader) {
    originalStream = inputStream;
    this.keyPath = keyPath;
    this.trailerHeader = Objects.requireNonNull(trailerHeader, "trailerHeader == null");
    if (trailerHeader == TrailerHeader.NONE) {
      throw new IllegalArgumentException("Unsigned chunked uploads require a checksum trailer");
    }
    this.payloadChecksum = trailerHeader.newChecksum();
  }

  @Override
  public int read() throws IOException {
    if (isFinalChunkEncountered || !ensureChunkPayload()) {
      return -1;
    }
    int value = originalStream.read();
    if (value == -1) {
      throw invalidBody("Truncated chunk payload");
    }
    payloadChecksum.update((byte) value);
    remainingData--;
    endChunkIfComplete();
    return value;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "b == null");
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException("Offset=" + off + " and len="
          + len + " don't match the array length of " + b.length);
    } else if (len == 0) {
      return 0;
    }
    int totalReadBytes = 0;
    while (totalReadBytes < len && !isFinalChunkEncountered && ensureChunkPayload()) {
      int readLength = originalStream.read(b, off + totalReadBytes,
          (int) Math.min(remainingData, len - totalReadBytes));
      if (readLength == -1) {
        throw invalidBody("Truncated chunk payload");
      }
      payloadChecksum.update(b, off + totalReadBytes, readLength);
      totalReadBytes += readLength;
      remainingData -= readLength;
      endChunkIfComplete();
    }
    return totalReadBytes > 0 ? totalReadBytes : -1;
  }

  private boolean ensureChunkPayload() throws IOException {
    if (remainingData > 0) {
      return true;
    }
    String size = readLine();
    if (!CHUNK_SIZE_PATTERN.matcher(size).matches()) {
      throw invalidBody("Invalid unsigned chunk size");
    }
    try {
      remainingData = Long.parseLong(size, 16);
    } catch (NumberFormatException ex) {
      throw invalidBody("Invalid unsigned chunk size");
    }
    if (remainingData > 0) {
      return true;
    }
    readTrailer();
    isFinalChunkEncountered = true;
    return false;
  }

  private void endChunkIfComplete() throws IOException {
    if (remainingData == 0 && (originalStream.read() != '\r' || originalStream.read() != '\n')) {
      throw invalidBody("Invalid chunk data terminator");
    }
  }

  /** Finish the zero-byte chunk and trailer after the caller consumes the decoded content length. */
  public void verifyComplete() throws IOException {
    if (!isFinalChunkEncountered && read() != -1) {
      throw invalidBody("Decoded content length ended before the chunked stream");
    }
  }

  /** @return the decoded checksum after successful trailer completion, or null before completion. */
  public byte[] getTrailerChecksum() {
    return trailerChecksum == null ? null : trailerChecksum.clone();
  }

  /** Check framing and decode the value, then compare the checksum only after the final terminator. */
  private void readTrailer() throws IOException {
    String trailer = readLine();
    int separator = trailer.indexOf(':');
    if (separator <= 0) {
      throw invalidBody("Missing or invalid trailing checksum header");
    }
    String name = trailer.substring(0, separator).trim().toLowerCase(Locale.ROOT);
    if (!trailerHeader.getHeaderName().equals(name)) {
      throw invalidBody("Unexpected trailing header: " + name);
    }
    byte[] checksum = trailerHeader.decodeValue(trailer.substring(separator + 1).trim(), keyPath);
    if (!readLine().isEmpty()) {
      throw invalidBody("Invalid trailer terminator");
    }
    if (originalStream.read() != -1) {
      throw invalidBody("Unexpected data after trailing checksum");
    }
    payloadChecksum.verify(checksum, keyPath);
    trailerChecksum = checksum;
  }

  private String readLine() throws IOException {
    StringBuilder line = new StringBuilder();
    int previous = -1;
    while (true) {
      int current = originalStream.read();
      if (current == -1) {
        throw invalidBody("Chunked stream ended before completion");
      }
      line.append((char) current);
      if (line.length() > MAX_LINE_LENGTH) {
        throw invalidBody("Chunk or trailing header line is too long");
      }
      if (eol(previous, current)) {
        line.setLength(line.length() - 2);
        return line.toString();
      }
      previous = current;
    }
  }

  private OS3Exception invalidBody(String message) {
    OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, keyPath);
    ex.setErrorMessage(message);
    return ex;
  }
}
