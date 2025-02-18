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

package org.apache.hadoop.ozone.s3.signature;

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;

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
  public SignatureInfo parseSignature() throws MalformedResourceException {
    if (authHeader == null || !authHeader.startsWith(IDENTIFIER + " ")) {
      return null;
    }
    String[] split = authHeader.split(" ");
    if (split.length != 2) {
      throw new MalformedResourceException(authHeader);
    }

    String identifier = split[0];
    if (!IDENTIFIER.equals(identifier)) {
      throw new MalformedResourceException(authHeader);
    }

    String[] remainingSplit = split[1].split(":");

    if (remainingSplit.length != 2) {
      throw new MalformedResourceException(authHeader);
    }

    String accessKeyID = remainingSplit[0];
    String signature = remainingSplit[1];
    if (isBlank(accessKeyID) || isBlank(signature)) {
      throw new MalformedResourceException(authHeader);
    }
    return new SignatureInfo.Builder(Version.V2)
        .setAwsAccessId(accessKeyID)
        .setSignature(signature)
        .build();
  }
}
