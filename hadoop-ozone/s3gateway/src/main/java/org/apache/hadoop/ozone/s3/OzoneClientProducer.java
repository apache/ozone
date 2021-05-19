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
package org.apache.hadoop.ozone.s3;

import javax.annotation.PreDestroy;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.apache.hadoop.ozone.s3.signature.SignatureProcessor;
import org.apache.hadoop.ozone.s3.signature.StringToSignProducer;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates the OzoneClient for the Rest endpoints.
 */
@RequestScoped
public class OzoneClientProducer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientProducer.class);

  private OzoneClient client;

  @Inject
  private SignatureProcessor signatureProcessor;

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private Text omService;

  @Inject
  private String omServiceID;

  @Context
  private ContainerRequestContext context;

  @Produces
  public OzoneClient createClient() throws WebApplicationException,
      IOException {
    client = getClient(ozoneConfiguration);
    return client;
  }

  @PreDestroy
  public void destroy() throws IOException {
    client.close();
  }

  private OzoneClient getClient(OzoneConfiguration config)
      throws WebApplicationException {
    OzoneClient ozoneClient = null;
    try {
      SignatureInfo signatureInfo = signatureProcessor.parseSignature();

      String stringToSign = "";
      if (signatureInfo.getVersion() == Version.V4) {
        stringToSign =
            StringToSignProducer.createSignatureBase(signatureInfo, context);
      }

      String awsAccessId = signatureInfo.getAwsAccessId();
      validateAccessId(awsAccessId);

      UserGroupInformation remoteUser =
          UserGroupInformation.createRemoteUser(awsAccessId);
      if (OzoneSecurityUtil.isSecurityEnabled(config)) {
        LOG.debug("Creating s3 auth info for client.");

        if (signatureInfo.getVersion() == Version.NONE) {
          throw MALFORMED_HEADER;
        }

        OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
        identifier.setTokenType(S3AUTHINFO);
        identifier.setStrToSign(stringToSign);
        identifier.setSignature(signatureInfo.getSignature());
        identifier.setAwsAccessId(awsAccessId);
        identifier.setOwner(new Text(awsAccessId));
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding token for service:{}", omService);
        }
        Token<OzoneTokenIdentifier> token = new Token(identifier.getBytes(),
            identifier.getSignature().getBytes(StandardCharsets.UTF_8),
            identifier.getKind(),
            omService);
        remoteUser.addToken(token);

      }
      ozoneClient =
          remoteUser.doAs((PrivilegedExceptionAction<OzoneClient>) () -> {
            return createOzoneClient();
          });
    } catch (OS3Exception ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error during Client Creation: ", ex);
      }
      throw wrapOS3Exception(ex);
    } catch (Throwable t) {
      // For any other critical errors during object creation throw Internal
      // error.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error during Client Creation: ", t);
      }
      throw wrapOS3Exception(INTERNAL_ERROR);
    }
    return ozoneClient;
  }

  @NotNull
  @VisibleForTesting
  OzoneClient createOzoneClient() throws IOException {
    if (omServiceID == null) {
      return OzoneClientFactory.getRpcClient(ozoneConfiguration);
    } else {
      // As in HA case, we need to pass om service ID.
      return OzoneClientFactory.getRpcClient(omServiceID,
          ozoneConfiguration);
    }
  }

  // ONLY validate aws access id when needed.
  private void validateAccessId(String awsAccessId) throws Exception {
    if (awsAccessId == null || awsAccessId.equals("")) {
      LOG.error("Malformed s3 header. awsAccessID: ", awsAccessId);
      throw wrapOS3Exception(MALFORMED_HEADER);
    }
  }

  public void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }

  @VisibleForTesting
  public void setSignatureParser(SignatureProcessor awsSignatureProcessor) {
    this.signatureProcessor = awsSignatureProcessor;
  }

  private WebApplicationException wrapOS3Exception(OS3Exception os3Exception) {
    return new WebApplicationException(os3Exception,
        os3Exception.getHttpCode());
  }
}
