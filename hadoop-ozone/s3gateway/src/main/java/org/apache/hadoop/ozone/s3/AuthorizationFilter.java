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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.apache.hadoop.ozone.s3.signature.SignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter used to construct string to sign from unfiltered request.
 * It should be executed before all other filters as the original
 * could be enriched later.
 */

@Provider
@PreMatching
@Priority(AuthorizationFilter.PRIORITY)
public class AuthorizationFilter implements ContainerRequestFilter {
  public static final int PRIORITY = 50;

  private static final Logger LOG = LoggerFactory.getLogger(
      AuthorizationFilter.class);

  @Inject
  private SignatureProcessor signatureProcessor;

  @Inject
  private SignatureInfo signatureInfo;

  @Override
  public void filter(ContainerRequestContext context) throws
      IOException {
    try {
      signatureInfo.initialize(signatureProcessor.parseSignature());
      if (signatureInfo.getVersion() == Version.V4) {
        signatureInfo.setStrToSign(
            StringToSignProducer.createSignatureBase(signatureInfo, context));
      } else {
        LOG.debug("Unsupported AWS signature version: {}",
            signatureInfo.getVersion());
        throw S3_AUTHINFO_CREATION_ERROR;
      }

      String awsAccessId = signatureInfo.getAwsAccessId();
      // ONLY validate aws access id when needed.
      if (awsAccessId == null || awsAccessId.equals("")) {
        LOG.debug("Malformed s3 header. awsAccessID: {}", awsAccessId);
        throw ACCESS_DENIED;
      }
    } catch (OS3Exception ex) {
      LOG.debug("Error during Client Creation: ", ex);
      throw wrapOS3Exception(ex);
    } catch (Exception e) {
      // For any other critical errors during object creation throw Internal
      // error.
      LOG.debug("Error during Client Creation: ", e);
      throw wrapOS3Exception(
          S3ErrorTable.newError(INTERNAL_ERROR, null, e));
    }
  }

  @VisibleForTesting
  public void setSignatureParser(SignatureProcessor awsSignatureProcessor) {
    this.signatureProcessor = awsSignatureProcessor;
  }

  @VisibleForTesting
  public void setSignatureInfo(SignatureInfo signatureInfo) {
    this.signatureInfo = signatureInfo;
  }

  @VisibleForTesting
  public SignatureInfo getSignatureInfo() {
    return signatureInfo;
  }

}
