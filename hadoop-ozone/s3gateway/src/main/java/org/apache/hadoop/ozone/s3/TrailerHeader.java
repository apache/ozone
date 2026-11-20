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

import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_TRAILER;

import java.util.Base64;
import java.util.Locale;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/** Supported checksum trailers, or no trailer for a regular signed stream. */
public enum TrailerHeader {
  NONE("", 0),
  CRC32("x-amz-checksum-crc32", 4),
  CRC32C("x-amz-checksum-crc32c", 4),
  CRC64NVME("x-amz-checksum-crc64nvme", 8),
  SHA1("x-amz-checksum-sha1", 20),
  SHA256("x-amz-checksum-sha256", 32);

  private final String headerName;
  private final int checksumLength;

  TrailerHeader(String headerName, int checksumLength) {
    this.headerName = headerName;
    this.checksumLength = checksumLength;
  }

  String getHeaderName() {
    return headerName;
  }

  TrailerChecksum newChecksum() {
    switch (this) {
    case NONE:
      return null;
    case CRC32:
      return TrailerChecksum.of(new CRC32(), checksumLength);
    case CRC32C:
      return TrailerChecksum.of(new CRC32C(), checksumLength);
    case CRC64NVME:
      return TrailerChecksum.of(new TrailerChecksum.Crc64Nvme(), checksumLength);
    case SHA1:
      return TrailerChecksum.of(DigestUtils.getSha1Digest());
    case SHA256:
      return TrailerChecksum.of(DigestUtils.getSha256Digest());
    default:
      throw new IllegalStateException("Unsupported trailer checksum: " + this);
    }
  }

  byte[] decodeValue(String value, String resource) {
    try {
      byte[] checksum = Base64.getDecoder().decode(value);
      if (checksum.length == checksumLength) {
        return checksum;
      }
    } catch (IllegalArgumentException ignored) {
      // Invalid Base64 and incorrect decoded length are both malformed checksum values.
    }
    OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, resource);
    ex.setErrorMessage("Value for " + headerName + " header is invalid");
    throw ex;
  }

  /** Parse the required single-checksum declaration of a trailer upload. */
  public static TrailerHeader fromHeader(String header, String keyPath) {
    if (StringUtils.isBlank(header)) {
      OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_ARGUMENT, keyPath);
      ex.setErrorMessage("The " + X_AMZ_TRAILER + " header is required for trailing headers");
      throw ex;
    }
    String name = header.trim().toLowerCase(Locale.ROOT);
    for (TrailerHeader trailer : values()) {
      if (trailer != NONE && trailer.headerName.equals(name)) {
        return trailer;
      }
    }
    OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, keyPath);
    ex.setErrorMessage("Invalid x-amz-trailer header");
    throw ex;
  }
}
