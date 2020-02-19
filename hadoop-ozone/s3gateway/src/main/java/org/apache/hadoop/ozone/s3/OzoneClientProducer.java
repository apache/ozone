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

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;
import static org.apache.hadoop.ozone.s3.SignatureProcessor.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.S3_AUTHINFO_CREATION_ERROR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates the OzoneClient for the Rest endpoints.
 */
@RequestScoped
public class OzoneClientProducer {

  private final static Logger LOG =
      LoggerFactory.getLogger(OzoneClientProducer.class);

  @Inject
  private SignatureProcessor v4RequestParser;

  @Inject
  private OzoneConfiguration ozoneConfiguration;

  @Inject
  private Text omService;

  @Inject
  private String omServiceID;


  @Produces
  public OzoneClient createClient() throws IOException {
    return getClient(ozoneConfiguration);
  }

  private OzoneClient getClient(OzoneConfiguration config) throws IOException {
    try {
      if (OzoneSecurityUtil.isSecurityEnabled(config)) {
        LOG.debug("Creating s3 auth info for client.");
        try {

          OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
          identifier.setTokenType(S3AUTHINFO);
          identifier.setStrToSign(v4RequestParser.getStringToSign());
          identifier.setSignature(v4RequestParser.getSignature());
          identifier.setAwsAccessId(v4RequestParser.getAwsAccessId());
          identifier.setOwner(new Text(v4RequestParser.getAwsAccessId()));
          if (LOG.isTraceEnabled()) {
            LOG.trace("Adding token for service:{}", omService);
          }
          Token<OzoneTokenIdentifier> token = new Token(identifier.getBytes(),
              identifier.getSignature().getBytes(UTF_8),
              identifier.getKind(),
              omService);
          UserGroupInformation remoteUser =
              UserGroupInformation.createRemoteUser(
                  v4RequestParser.getAwsAccessId());
          remoteUser.addToken(token);
          UserGroupInformation.setLoginUser(remoteUser);
        } catch (OS3Exception | URISyntaxException ex) {
          LOG.error("S3 auth info creation failed.");
          throw S3_AUTHINFO_CREATION_ERROR;
        }

      }
    } catch (Exception e) {
      LOG.error("Error: ", e);
    }

    if (omServiceID == null) {
      return OzoneClientFactory.getClient(ozoneConfiguration);
    } else {
      // As in HA case, we need to pass om service ID.
      return OzoneClientFactory.getRpcClient(omServiceID, ozoneConfiguration);
    }
  }

  public void setOzoneConfiguration(OzoneConfiguration config) {
    this.ozoneConfiguration = config;
  }
}
