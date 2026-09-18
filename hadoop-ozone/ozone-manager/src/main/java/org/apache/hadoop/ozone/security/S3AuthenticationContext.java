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

package org.apache.hadoop.ozone.security;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;

/**
 * Captures and installs S3 authentication state associated with the current
 * thread.
 */
public final class S3AuthenticationContext {
  private static final S3AuthenticationContext EMPTY = new S3AuthenticationContext(null, null);

  private final S3Authentication s3Authentication;
  private final STSTokenIdentifier stsTokenIdentifier;

  private S3AuthenticationContext(
      S3Authentication s3Authentication, STSTokenIdentifier stsTokenIdentifier) {
    this.s3Authentication = s3Authentication;
    this.stsTokenIdentifier = stsTokenIdentifier;
  }

  public static S3AuthenticationContext capture() {
    return new S3AuthenticationContext(
        OzoneManager.getS3Auth(), OzoneManager.getStsTokenIdentifier());
  }

  public static void captureInto(OMRequest.Builder requestBuilder) {
    if (requestBuilder.hasS3Authentication()) {
      requestBuilder.setS3Authentication(resolve(
          requestBuilder.getS3Authentication(), OzoneManager.getStsTokenIdentifier()));
    }
  }

  public static S3AuthenticationContext fromRequest(
      OMRequest request, boolean securityEnabled) throws IOException {
    if (!securityEnabled || !request.hasS3Authentication()) {
      return EMPTY;
    }

    STSSecurityUtil.ensureResolvedStsFieldsInvariants(request);
    S3Authentication s3Auth = request.getS3Authentication();
    STSTokenIdentifier stsToken = hasSessionToken(s3Auth)
        ? rehydrateStsTokenIdentifier(s3Auth) : null;
    return new S3AuthenticationContext(s3Auth, stsToken);
  }

  public void install() {
    OzoneManager.setS3Auth(s3Authentication);
    OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
  }

  public static void clear() {
    EMPTY.install();
  }

  private static S3Authentication resolve(
      S3Authentication s3Auth, STSTokenIdentifier stsTokenIdentifier) {
    S3Authentication.Builder s3AuthBuilder = s3Auth.toBuilder();

    if (hasSessionToken(s3Auth) && stsTokenIdentifier != null) {
      s3AuthBuilder.setResolvedStsSessionPolicy(
          StringUtils.defaultString(stsTokenIdentifier.getSessionPolicy()));
      s3AuthBuilder.setResolvedStsRoleArn(
          StringUtils.defaultString(stsTokenIdentifier.getRoleArn()));
      s3AuthBuilder.setResolvedStsOriginalAccessKeyId(
          StringUtils.defaultString(stsTokenIdentifier.getOriginalAccessKeyId()));
      s3AuthBuilder.setResolvedStsTempAccessKeyId(
          StringUtils.defaultString(stsTokenIdentifier.getTempAccessKeyId()));
      UUID secretKeyId = stsTokenIdentifier.getSecretKeyId();
      s3AuthBuilder.setResolvedStsSecretKeyId(
          secretKeyId != null ? secretKeyId.toString() : "");
    } else {
      s3AuthBuilder.clearResolvedStsSessionPolicy();
      s3AuthBuilder.clearResolvedStsRoleArn();
      s3AuthBuilder.clearResolvedStsOriginalAccessKeyId();
      s3AuthBuilder.clearResolvedStsTempAccessKeyId();
      s3AuthBuilder.clearResolvedStsSecretKeyId();
    }

    return s3AuthBuilder.build();
  }

  private static boolean hasSessionToken(S3Authentication s3Auth) {
    return s3Auth.hasSessionToken() && !s3Auth.getSessionToken().isEmpty();
  }

  private static STSTokenIdentifier rehydrateStsTokenIdentifier(S3Authentication s3Auth) {
    // Context reaching Ratis has already passed expiry and revocation checks.
    return new STSTokenIdentifier(STSTokenIdentifier.Params.newBuilder()
        .setTempAccessKeyId(s3Auth.hasResolvedStsTempAccessKeyId() ? s3Auth.getResolvedStsTempAccessKeyId() : "")
        .setOriginalAccessKeyId(
            s3Auth.hasResolvedStsOriginalAccessKeyId() ? s3Auth.getResolvedStsOriginalAccessKeyId() : "")
        .setRoleArn(s3Auth.hasResolvedStsRoleArn() ? s3Auth.getResolvedStsRoleArn() : "")
        .setCreationTime(Instant.MAX)
        .setExpiry(Instant.MAX)
        .setSecretAccessKey(null)
        .setSessionPolicy(s3Auth.hasResolvedStsSessionPolicy() ? s3Auth.getResolvedStsSessionPolicy() : "")
        .setManagedSecretKey(null)
        .build());
  }
}
