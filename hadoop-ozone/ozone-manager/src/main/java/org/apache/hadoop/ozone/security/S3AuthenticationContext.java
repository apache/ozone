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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;

/**
 * Captures and applies S3 authentication state associated with the current
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

  public static void captureInto(OMRequest.Builder requestBuilder, OzoneManager ozoneManager)
      throws IOException {
    if (requestBuilder.hasS3Authentication()) {
      requestBuilder.setS3Authentication(
          STSSecurityUtil.resolveS3Authentication(requestBuilder.getS3Authentication(), ozoneManager));
    }
  }

  public static S3AuthenticationContext fromRequest(
      OMRequest request, boolean securityEnabled) throws IOException {
    if (!securityEnabled || !request.hasS3Authentication()) {
      return EMPTY;
    }

    STSSecurityUtil.ensureResolvedStsFieldsInvariants(request);
    S3Authentication s3Auth = request.getS3Authentication();
    STSTokenIdentifier stsToken = STSSecurityUtil.rehydrateStsTokenIdentifier(s3Auth);
    return new S3AuthenticationContext(s3Auth, stsToken);
  }

  public void applyToCurrentThread() {
    OzoneManager.setS3Auth(s3Authentication);
    OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
  }

  public static void clear() {
    EMPTY.applyToCurrentThread();
  }

}
