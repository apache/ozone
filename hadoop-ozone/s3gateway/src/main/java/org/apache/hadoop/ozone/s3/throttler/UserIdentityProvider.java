/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.throttler;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.AuthorizationV2HeaderParser;
import org.apache.hadoop.ozone.s3.signature.AuthorizationV4HeaderParser;
import org.apache.hadoop.ozone.s3.signature.AuthorizationV4QueryParser;
import org.apache.hadoop.ozone.s3.signature.AWSSignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.MalformedResourceException;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureParser;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;

import javax.ws.rs.core.MultivaluedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of IdentityProvider to provide user from a given headers.
 */
public class UserIdentityProvider implements IdentityProvider {
  @Override
  public String makeIdentity(Request request) throws OS3Exception {
    Map<String, String> rawHeaders = request.getHeaders();
    Map<String, String> queryParams = request.getQueryParameters();

    AWSSignatureProcessor.LowerCaseKeyStringMap headers =
        AWSSignatureProcessor.LowerCaseKeyStringMap.fromHeaderMap(
            new MultivaluedHashMap<>(rawHeaders));

    String authHeader = headers.get("Authorization");

    List<SignatureParser> signatureParsers = new ArrayList<>();
    signatureParsers.add(new AuthorizationV4HeaderParser(authHeader,
        headers.get(StringToSignProducer.X_AMAZ_DATE)));
    signatureParsers.add(new AuthorizationV4QueryParser(
        StringToSignProducer.fromMultiValueToSingleValueMap(
            new MultivaluedHashMap<>(queryParams))));
    signatureParsers.add(new AuthorizationV2HeaderParser(authHeader));

    SignatureInfo signatureInfo = null;
    for (SignatureParser parser : signatureParsers) {
      try {
        signatureInfo = parser.parseSignature();
      } catch (MalformedResourceException e) {
        throw S3ErrorTable.MALFORMED_HEADER;
      }
      if (signatureInfo != null) {
        break;
      }
    }
    if (signatureInfo == null) {
      throw S3ErrorTable.MALFORMED_HEADER;
    }
    return signatureInfo.getAwsAccessId().toLowerCase();
  }
}
