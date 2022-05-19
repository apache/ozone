/**
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
package org.apache.hadoop.ozone.s3.signature;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Class to parse V2 auth information from header.
 */
public class AuthorizationV2HeaderParser implements SignatureParser {

  public static final String IDENTIFIER = "AWS";

  private final String authHeader;

  public AuthorizationV2HeaderParser(String authHeader) {
    this.authHeader = authHeader;
  }

  /**
   * This method parses the authorization header.
   * <p>
   * Authorization header sample:
   * AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=
   */
  @Override
  public SignatureInfo parseSignature() throws OS3Exception {
    if (authHeader == null || !authHeader.startsWith(IDENTIFIER + " ")) {
      return null;
    }
    String[] split = authHeader.split(" ");
    if (split.length != 2) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    String identifier = split[0];
    if (!IDENTIFIER.equals(identifier)) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    String[] remainingSplit = split[1].split(":");

    if (remainingSplit.length != 2) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }

    String accessKeyID = remainingSplit[0];
    String signature = remainingSplit[1];
    if (isBlank(accessKeyID) || isBlank(signature)) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, authHeader);
    }
    return new SignatureInfo(
        Version.V2,
        "",
        "",
        accessKeyID,
        signature,
        "",
        "",
        "",
        false
    );
  }
}
