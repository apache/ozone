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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.GrpcOmTransport;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cached ozone client for s3 requests.
 */
@Singleton
public class OzoneClientCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneClientCache.class);

  private final OzoneConfiguration conf;
  private OzoneClient client;

  @Inject
  OzoneClientCache(OzoneConfiguration conf) {
    this.conf = conf;
    LOG.debug("{}: Created", this);
  }

  @PostConstruct
  public void initialize() throws IOException {
    conf.set("ozone.om.group.rights", "NONE");
    // Set the expected OM version if not set via config.
    conf.setIfUnset(OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_KEY,
        OZONE_CLIENT_REQUIRED_OM_VERSION_MIN_DEFAULT);
    conf.setBoolean(S3Auth.S3_AUTH_CHECK, true);

    client = createClient(conf);
    Preconditions.assertTrue(conf.getBoolean(S3Auth.S3_AUTH_CHECK, false), S3Auth.S3_AUTH_CHECK);

    LOG.debug("{}: Initialized", this);
  }

  public OzoneClient getClient() {
    return client;
  }

  public static OzoneClient createClient(OzoneConfiguration ozoneConfiguration) throws IOException {
    String omServiceID = OmUtils.getOzoneManagerServiceId(ozoneConfiguration);
    SecurityConfig secConfig = new SecurityConfig(ozoneConfiguration);
    try {
      if (secConfig.isGrpcTlsEnabled()) {
        if (ozoneConfiguration
            .get(OZONE_OM_TRANSPORT_CLASS,
                OZONE_OM_TRANSPORT_CLASS_DEFAULT) !=
            OZONE_OM_TRANSPORT_CLASS_DEFAULT) {
          // Grpc transport selected
          // need to get certificate for TLS through
          // hadoop rpc first via ServiceInfo
          setCertificate(secConfig, omServiceID, ozoneConfiguration);
        }
      }
      if (omServiceID == null) {
        return OzoneClientFactory.getRpcClient(ozoneConfiguration);
      } else {
        // As in HA case, we need to pass om service ID.
        return OzoneClientFactory.getRpcClient(omServiceID,
            ozoneConfiguration);
      }
    } catch (IOException e) {
      LOG.warn("cannot create OzoneClient", e);
      throw e;
    }
  }

  private static void setCertificate(SecurityConfig secConfig, String omServiceID, OzoneConfiguration conf)
      throws IOException {

    // create local copy of config incase exception occurs
    // with certificate OmRequest
    OzoneConfiguration config = new OzoneConfiguration(conf);
    OzoneClient certClient;

    if (secConfig.isGrpcTlsEnabled()) {
      // set OmTransport to hadoop rpc to securely,
      // get certificates with service list request
      config.set(OZONE_OM_TRANSPORT_CLASS,
          OZONE_OM_TRANSPORT_CLASS_DEFAULT);
      config.setBoolean(S3Auth.S3_AUTH_CHECK, false);

      if (omServiceID == null) {
        certClient = OzoneClientFactory.getRpcClient(config);
      } else {
        // As in HA case, we need to pass om service ID.
        certClient = OzoneClientFactory.getRpcClient(omServiceID,
            config);
      }
      try {
        ServiceInfoEx serviceInfoEx = certClient
            .getObjectStore()
            .getClientProxy()
            .getOzoneManagerClient()
            .getServiceInfo();

        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          String caCertPem = null;
          List<String> caCertPems = null;
          caCertPem = serviceInfoEx.getCaCertificate();
          caCertPems = serviceInfoEx.getCaCertPemList();
          if (caCertPems == null || caCertPems.isEmpty()) {
            if (caCertPem == null) {
              LOG.error("S3g received empty caCertPems from serviceInfo");
              throw new CertificateException("No caCerts found; caCertPem can" +
                  " not be null when caCertPems is empty or null");
            }
            caCertPems = Collections.singletonList(caCertPem);
          }
          GrpcOmTransport.setCaCerts(OzoneSecurityUtil
              .convertToX509(caCertPems));
        }
      } catch (CertificateException ce) {
        throw new IOException(ce);
      } finally {
        if (certClient != null) {
          certClient.close();
        }
      }
    }
  }

  @PreDestroy
  public void cleanup() {
    LOG.debug("{}: Closing cached client", this);
    IOUtils.close(LOG, client);
  }
}
