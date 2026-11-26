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

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.s3.signature.ChunksValidator;

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
 * \r\n
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
 * {@link AwsChunkedInputStream} does the parsing shared with {@link UnsignedChunksInputStream}. This class also
 * parses the "chunk-signature" of each chunk.
 * </p>
 *
 * <p>
 * If a {@link ChunksValidator} is supplied, the signature of each chunk is verified (in real time, without
 * buffering the whole chunk) as the payload is read. Without a validator the stream only strips the chunk
 * signature framing and returns the payload. When a checksum trailer is declared, its framing and payload
 * checksum are verified even without a validator; chunk and trailer signatures are only shape-checked in that case.
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
public class SignedChunksInputStream extends AwsChunkedInputStream {

  /**
   * Chunk header line. The signature is HMAC-SHA256, i.e. exactly 64 hex characters: the S3 Gateway only
   * accepts the AWS4-HMAC-SHA256 signing algorithm, so this is the only chunk signature format it can see.
   */
  private static final Pattern SIGNATURE_LINE_PATTERN =
      Pattern.compile("([0-9A-Fa-f]+);chunk-signature=([0-9A-Fa-f]{64})");
  private static final Pattern TRAILER_SIGNATURE_PATTERN =
      Pattern.compile("x-amz-trailer-signature:([0-9A-Fa-f]{64})", Pattern.CASE_INSENSITIVE);

  /** Verifies each chunk signature, or {@code null} to skip verification. */
  private ChunksValidator validator;

  /** SHA-256 of the current chunk payload; {@code null} when not verifying. */
  private MessageDigest chunkDigest;

  /** Signature parsed from the current chunk header line. */
  private String chunkSignature;

  public SignedChunksInputStream(InputStream inputStream, String keyPath) {
    this(inputStream, keyPath, TrailerHeader.NONE);
  }

  /**
   * Creates a signed chunk stream. Supports one checksum trailer; comma-separated trailer names are not supported.
   *
   * @param inputStream the encoded request body
   * @param keyPath resource used in S3 errors
   * @param trailerHeader the checksum trailer, or NONE when no trailer is expected
   */
  public SignedChunksInputStream(InputStream inputStream, String keyPath, TrailerHeader trailerHeader) {
    super(inputStream, keyPath, trailerHeader);
  }

  /**
   * Enable chunk-signature verification. Used when the signing key (derived by
   * OM, see HDDS-15140) only becomes available after the key is opened, i.e.
   * after this stream is constructed. Must be called before the first read.
   */
  public void attachValidator(ChunksValidator chunksValidator) {
    this.validator = Objects.requireNonNull(chunksValidator, "chunksValidator == null");
    this.chunkDigest = newSha256();
  }

  /**
   * @return the signed chunk stream the request body is built on, or {@code null} if the body is not
   *         a signed multi-chunk upload.
   */
  public static SignedChunksInputStream unwrap(InputStream body) {
    if (body instanceof MultiDigestInputStream) {
      InputStream wrapped = ((MultiDigestInputStream) body).getWrappedStream();
      if (wrapped instanceof SignedChunksInputStream) {
        return (SignedChunksInputStream) wrapped;
      }
    }
    return null;
  }

  @Override
  boolean requiresCompletion() {
    return validator != null || super.requiresCompletion();
  }

  @Override
  long parseChunkSize(String signatureLine) {
    // Example of a single chunk data:
    //  10000;chunk-signature=b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2\r\n
    //  <65536-bytes>\r\n
    //
    // 10000 will be read and decoded from base-16 representation to 65536, which is the size of
    // the subsequent chunk payload.
    signatureLine = signatureLine.trim();
    if (signatureLine.isEmpty()) {
      return -1;
    }

    //parse the data length and the chunk signature.
    Matcher matcher = SIGNATURE_LINE_PATTERN.matcher(signatureLine);
    if (!matcher.matches()) {
      throw invalidBody("Invalid signature line: " + signatureLine);
    }
    chunkSignature = matcher.group(2);
    return Integer.parseInt(matcher.group(1), 16);
  }

  @Override
  void readTrailerSignature(String name, String value) throws IOException {
    String signatureLine = readTrailerLine();
    if (signatureLine == null) {
      throw invalidBody("Missing trailing signature");
    }
    Matcher matcher = TRAILER_SIGNATURE_PATTERN.matcher(signatureLine.trim());
    if (!matcher.matches()) {
      throw invalidBody("Invalid trailing signature");
    }
    if (validator != null) {
      validator.validateTrailer(matcher.group(1), DigestUtils.sha256Hex(name + ":" + value + "\n"));
    }
  }

  @Override
  void updateDigest(byte b) {
    super.updateDigest(b);
    if (chunkDigest != null) {
      chunkDigest.update(b);
    }
  }

  @Override
  void updateDigest(byte[] b, int off, int len) {
    super.updateDigest(b, off, len);
    if (chunkDigest != null) {
      chunkDigest.update(b, off, len);
    }
  }

  /**
   * Verify the signature of the chunk just read. {@link MessageDigest#digest()}
   * also resets the digest for the next chunk. No-op without a validator.
   */
  @Override
  void validateChunk() {
    if (validator != null) {
      validator.validateChunk(chunkSignature,
          DatatypeConverter.printHexBinary(chunkDigest.digest()).toLowerCase(Locale.ROOT));
    }
  }

  /**
   * A dedicated instance rather than {@code EndpointBase.getSha256DigestInstance()}: that is a shared
   * ThreadLocal, and {@link MultiDigestInputStream} may already be using it for the request's own SHA-256.
   */
  private static MessageDigest newSha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
