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

import javax.ws.rs.core.MultivaluedMap;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser for getting auth info from query parameters.
 * <p>
 * See: https://docs.aws.amazon
 * .com/AmazonS3/latest/API/sigv4-query-string-auth.html
 */
public class AuthorizationV4QueryParser implements SignatureParser {

  private static final Logger LOG =
      LoggerFactory.getLogger(AuthorizationV4QueryParser.class);

  private final MultivaluedMap<String, String> queryParameters;

  public AuthorizationV4QueryParser(
      MultivaluedMap<String, String> queryParameters
  ) {
    this.queryParameters = queryParameters;
  }

  @Override
  public SignatureInfo parseSignature() throws OS3Exception {

    if (!queryParameters.containsKey("X-Amz-Signature")) {
      return null;
    }

    Credential credential =
        new Credential(queryParameters.getFirst("X-Amz-Credential"));

    return new SignatureInfo(
        Version.V4,
        queryParameters.getFirst("X-Amz-Date"),
        credential.getAccessKeyID(),
        queryParameters.getFirst("X-Amz-Signature"),
        queryParameters.getFirst("X-Amz-SignedHeaders"),
        credential.createScope(),
        queryParameters.getFirst("X-Amz-Algorithm")
    );
  }
}
