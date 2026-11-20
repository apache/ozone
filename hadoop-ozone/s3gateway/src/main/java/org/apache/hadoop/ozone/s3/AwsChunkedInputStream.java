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
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Input stream implementation to read the payload of an aws-chunked request body, shared by
 * {@link SignedChunksInputStream} and {@link UnsignedChunksInputStream}. It reads the chunk payloads, the final
 * zero-byte chunk and the checksum trailer, if one is declared. Subclasses parse the chunk header line and may
 * verify chunk and trailer signatures.
 */
public abstract class AwsChunkedInputStream extends InputStream {

  private static final int MAX_LINE_LENGTH = 8 * 1024;

  private final InputStream originalStream;

  /** Identifies the object in the S3 error response for a malformed body. */
  private final String keyPath;

  /** Checksum header declared by x-amz-trailer, or NONE for a regular signed stream. */
  private final TrailerHeader trailerHeader;
  private final TrailerChecksum payloadChecksum;

  /**
   * Size of the chunk payload. If zero, the chunk header line should be parsed to
   * retrieve the subsequent chunk payload size.
   */
  private long remainingData = 0;

  /**
   * Every chunked uploads (multiple chunks) contains an additional final zero-byte
   * chunk. This can be used as the end-of-file marker.
   */
  private boolean isFinalChunkEncountered = false;

  private byte[] trailerChecksum;

  AwsChunkedInputStream(InputStream inputStream, String keyPath, TrailerHeader trailerHeader) {
    originalStream = inputStream;
    this.keyPath = keyPath;
    this.trailerHeader = Objects.requireNonNull(trailerHeader, "trailerHeader == null");
    this.payloadChecksum = trailerHeader.newChecksum();
  }

  /**
   * Parse a chunk header line.
   *
   * @return the size of the chunk payload, or -1 if the line ends the body without the final zero-byte chunk
   */
  abstract long parseChunkSize(String line);

  /** Verify the chunk just read, once its payload is fully consumed. */
  void validateChunk() {
  }

  /** Read and verify the trailer signature, which follows the checksum header in a signed trailer. */
  void readTrailerSignature(String name, String value) throws IOException {
  }

  /**
   * A stream with a declared trailer checksum must be complete and well-formed. Otherwise a truncated body or an
   * invalid chunk data terminator is tolerated.
   */
  boolean requiresCompletion() {
    return payloadChecksum != null;
  }

  void updateDigest(byte b) {
    if (payloadChecksum != null) {
      payloadChecksum.update(b);
    }
  }

  void updateDigest(byte[] b, int off, int len) {
    if (payloadChecksum != null) {
      payloadChecksum.update(b, off, len);
    }
  }

  @Override
  public int read() throws IOException {
    if (isFinalChunkEncountered || !ensureChunkPayload()) {
      return -1;
    }
    int curr = originalStream.read();
    if (curr == -1) {
      stopAtUnexpectedEof();
      return -1;
    }
    remainingData--;
    updateDigest((byte) curr);
    endChunkIfComplete();
    return curr;
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
    int currentOff = off;
    int currentLen = len;
    int totalReadBytes = 0;
    while (currentLen > 0 && !isFinalChunkEncountered && ensureChunkPayload()) {
      // The chunk payload size has been decoded, now read the actual chunk payload
      int realReadLen = originalStream.read(b, currentOff, (int) Math.min(remainingData, currentLen));
      if (realReadLen == -1) {
        stopAtUnexpectedEof();
        break;
      }
      updateDigest(b, currentOff, realReadLen);
      currentOff += realReadLen;
      currentLen -= realReadLen;
      totalReadBytes += realReadLen;
      remainingData -= realReadLen;
      endChunkIfComplete();
    }
    return totalReadBytes > 0 ? totalReadBytes : -1;
  }

  /**
   * Make sure a chunk payload is available to read, reading the next chunk header if the previous chunk was
   * fully consumed. Returns false at the end of the chunked stream: either the terminating zero-byte chunk
   * (verified here, since it has an empty payload) or a body that ends without one.
   */
  private boolean ensureChunkPayload() throws IOException {
    if (remainingData > 0) {
      return true;
    }
    String headerLine = readLine(false);
    remainingData = headerLine == null ? -1 : parseChunkSize(headerLine);
    if (remainingData > 0) {
      return true;
    }
    if (remainingData == 0) {
      // The final zero-byte chunk has no payload terminator when trailing headers follow it.
      if (trailerHeader == TrailerHeader.NONE) {
        readChunkTerminator();
      }
      validateChunk();
      if (trailerHeader != TrailerHeader.NONE) {
        readTrailer();
      }
      isFinalChunkEncountered = true;
    } else {
      stopAtUnexpectedEof();
    }
    return false;
  }

  /** Read the "\r\n" after the payload and verify the chunk, once its payload is fully consumed. */
  private void endChunkIfComplete() throws IOException {
    if (remainingData == 0) {
      readChunkTerminator();
      validateChunk();
    }
  }

  /**
   * A malformed chunked body is the client's error, so it must not surface as an InternalError.
   * The message is specific; the S3 error code is the generic InvalidRequest (HTTP 400).
   */
  OS3Exception invalidBody(String message) {
    OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, keyPath);
    ex.setErrorMessage(message);
    return ex;
  }

  private void stopAtUnexpectedEof() throws IOException {
    checkNotTruncated();
    isFinalChunkEncountered = true;
  }

  /**
   * Complete verification after the caller has consumed the decoded content length. This verifies the terminating
   * zero-byte chunk and rejects a decoded length that ends in the middle of a chunk.
   * Required when verifying signatures or a declared trailer checksum.
   */
  public void verifyComplete() throws IOException {
    if (!requiresCompletion() || isFinalChunkEncountered) {
      return;
    }
    if (read() != -1) {
      throw invalidBody("Decoded content length ended before the chunked stream");
    }
  }

  /** @return the decoded checksum after successful trailer completion, or null before completion. */
  public byte[] getTrailerChecksum() {
    return trailerChecksum == null ? null : trailerChecksum.clone();
  }

  /**
   * A verified stream must end with the terminating 0-byte chunk. Reaching EOF before it means the
   * body was truncated, so the payload could not be fully verified. Unverified streams without a
   * declared trailer keep their lenient behavior.
   */
  private void checkNotTruncated() throws IOException {
    if (requiresCompletion()) {
      throw invalidBody("Chunked stream ended before the terminating 0-byte chunk");
    }
  }

  private void readChunkTerminator() throws IOException {
    int carriageReturn = originalStream.read();
    int lineFeed = originalStream.read();
    if (requiresCompletion() && (carriageReturn != '\r' || lineFeed != '\n')) {
      throw invalidBody("Invalid chunk data terminator");
    }
  }

  /** Check framing, authenticate the trailer if possible, then compare its checksum after the final terminator. */
  private void readTrailer() throws IOException {
    String trailerLine = readTrailerLine();
    if (trailerLine == null || trailerLine.isEmpty()) {
      throw invalidBody("Missing trailing checksum header");
    }
    int separator = trailerLine.indexOf(':');
    if (separator <= 0) {
      throw invalidBody("Invalid trailing header: " + trailerLine);
    }
    String name = trailerLine.substring(0, separator).trim().toLowerCase(Locale.ROOT);
    String value = trailerLine.substring(separator + 1).trim();
    if (!trailerHeader.getHeaderName().equals(name)) {
      throw invalidBody("Unexpected trailing header: " + name);
    }
    readTrailerSignature(name, value);
    byte[] expected = trailerHeader.decodeValue(value, keyPath);
    // AWS SDKs terminate the trailer section with a blank line.
    if (!"".equals(readTrailerLine())) {
      throw invalidBody("Invalid trailer terminator");
    }
    if (originalStream.read() != -1) {
      throw invalidBody("Unexpected data after trailer terminator");
    }
    payloadChecksum.verify(expected, keyPath);
    trailerChecksum = expected;
  }

  /** Read a line of the trailer section, or null at the end of the stream. */
  String readTrailerLine() throws IOException {
    return readLine(true);
  }

  private String readLine(boolean trailingHeader) throws IOException {
    StringBuilder line = new StringBuilder();
    int previous = -1;
    while (true) {
      int current = originalStream.read();
      if (current == -1) {
        if (trailingHeader && requiresCompletion() && line.length() > 0) {
          throw invalidBody("Truncated trailing header");
        }
        if (!trailingHeader) {
          checkNotTruncated();
        }
        return line.length() == 0 ? null : line.toString();
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
}
