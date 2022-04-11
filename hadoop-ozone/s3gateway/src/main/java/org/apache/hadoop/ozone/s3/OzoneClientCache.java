/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_CLIENT_PROTOCOL_VERSION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_CLIENT_PROTOCOL_VERSION_KEY;

/**
 * Cached ozone client for s3 requests.
 */
@ApplicationScoped
public final class OzoneClientCache {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientCache.class);
  // single, cached OzoneClient established on first connection
  // for s3g gRPC OmTransport, OmRequest - OmResponse channel
  private static OzoneClientCache instance;
  private OzoneClient client;

  private OzoneClientCache(OzoneConfiguration ozoneConfiguration)
      throws IOException {
    // S3 Gateway should always set the S3 Auth.
    ozoneConfiguration.setBoolean(S3Auth.S3_AUTH_CHECK, true);
    // Set the expected OM version if not set via config.
    ozoneConfiguration.setIfUnset(OZONE_OM_CLIENT_PROTOCOL_VERSION_KEY,
        OZONE_OM_CLIENT_PROTOCOL_VERSION);
    String omServiceID = OmUtils.getOzoneManagerServiceId(ozoneConfiguration);
    try {
      if (omServiceID == null) {
        client = OzoneClientFactory.getRpcClient(ozoneConfiguration);
      } else {
        // As in HA case, we need to pass om service ID.
        client = OzoneClientFactory.getRpcClient(omServiceID,
            ozoneConfiguration);
      }
    } catch (IOException e) {
      LOG.warn("cannot create OzoneClient", e);
      throw e;
    }
  }

  public static OzoneClient getOzoneClientInstance(OzoneConfiguration
                                                      ozoneConfiguration)
      throws IOException {
    if (instance == null) {
      instance = new OzoneClientCache(ozoneConfiguration);
    }
    return instance.client;
  }

  @PreDestroy
  public void destroy() throws IOException {
    client.close();
  }
}
