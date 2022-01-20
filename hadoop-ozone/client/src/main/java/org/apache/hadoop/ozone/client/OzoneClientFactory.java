/**
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

package org.apache.hadoop.ozone.client;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class to create OzoneClients.
 */
public final class OzoneClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneClientFactory.class);

  /**
   * Private constructor, class is not meant to be initialized.
   */
  private OzoneClientFactory(){}


  /**
   * Constructs and return an OzoneClient with default configuration.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient() throws IOException {
    LOG.info("Creating OzoneClient with default configuration.");
    return getRpcClient(new OzoneConfiguration());
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @param omRpcPort
   *        RPC port of OzoneManager.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost, Integer omRpcPort,
      MutableConfigurationSource config)
      throws IOException {
    Preconditions.checkNotNull(omHost);
    Preconditions.checkNotNull(omRpcPort);
    Preconditions.checkNotNull(config);
    config.set(OZONE_OM_ADDRESS_KEY, omHost + ":" + omRpcPort);
    return getRpcClient(getClientProtocol(config), config);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omServiceId
   *        Service ID of OzoneManager HA cluster.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omServiceId,
      ConfigurationSource config) throws IOException {
    Preconditions.checkNotNull(omServiceId);
    Preconditions.checkNotNull(config);
    if (OmUtils.isOmHAServiceId(config, omServiceId)) {
      return getRpcClient(getClientProtocol(config, omServiceId), config);
    } else {
      throw new IOException("Service ID specified " +
          "does not match with " + OZONE_OM_SERVICE_IDS_KEY + " defined in " +
          "the configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are" +
          config.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY));
    }
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param config
   *        used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(ConfigurationSource config)
      throws IOException {
    Preconditions.checkNotNull(config);

    // Doing this explicitly so that when service ids are defined in the
    // configuration, we don't fall back to default ozone.om.address defined
    // in ozone-default.xml.

    String[] serviceIds = config.getTrimmedStrings(OZONE_OM_SERVICE_IDS_KEY);
    if (serviceIds.length > 1) {
      throw new IOException("Following ServiceID's " +
          config.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY) + " are" +
          " defined in the configuration. Use the method getRpcClient which " +
          "takes serviceID and configuration as param");
    } else if (serviceIds.length == 1) {
      return getRpcClient(getClientProtocol(config, serviceIds[0]), config);
    } else {
      return getRpcClient(getClientProtocol(config), config);
    }
  }

  /**
   * Creates OzoneClient with the given ClientProtocol and Configuration.
   *
   * @param clientProtocol
   *        Protocol to be used by the OzoneClient
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   */
  private static OzoneClient getRpcClient(ClientProtocol clientProtocol,
                                       ConfigurationSource config) {
    return new OzoneClient(config, clientProtocol);
  }

  /**
   * Create OzoneClient for token renew/cancel operations.
   * @param conf Configuration to be used for OzoneCient creation
   * @param token ozone token is involved
   * @return
   * @throws IOException
   */
  public static OzoneClient getOzoneClient(Configuration conf,
      Token<OzoneTokenIdentifier> token) throws IOException {
    Preconditions.checkNotNull(token, "Null token is not allowed");
    OzoneTokenIdentifier tokenId = new OzoneTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(
        token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    tokenId.readFields(in);
    String omServiceId = tokenId.getOmServiceId();
    OzoneConfiguration ozoneConf = OzoneConfiguration.of(conf);
    // Must check with OzoneConfiguration so that ozone-site.xml is loaded.
    if (StringUtils.isNotEmpty(omServiceId)) {
      // new OM should always issue token with omServiceId
      if (!OmUtils.isServiceIdsDefined(ozoneConf)
          && omServiceId.equals(OzoneConsts.OM_SERVICE_ID_DEFAULT)) {
        // Non-HA or single-node Ratis HA
        return OzoneClientFactory.getRpcClient(ozoneConf);
      } else if (OmUtils.isOmHAServiceId(ozoneConf, omServiceId)) {
        // HA with matching service id
        return OzoneClientFactory.getRpcClient(omServiceId, ozoneConf);
      } else {
        // HA with mismatched service id
        throw new IOException("Service ID specified " + omServiceId +
            " does not match" + " with " + OZONE_OM_SERVICE_IDS_KEY +
            " defined in the " + "configuration. Configured " +
            OZONE_OM_SERVICE_IDS_KEY + " are" +
            ozoneConf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY));
      }
    } else {
      // Old OM may issue token without omServiceId that should work
      // with non-HA case
      if (!OmUtils.isServiceIdsDefined(ozoneConf)) {
        return OzoneClientFactory.getRpcClient(ozoneConf);
      } else {
        throw new IOException("OzoneToken with no service ID can't "
            + "be renewed or canceled with local OM HA setup because we "
            + "don't know if the token is issued from local OM HA cluster "
            + "or not.");
      }
    }
  }

  /**
   * Returns an instance of Protocol class.
   *
   *
   * @param config
   *        Configuration used to initialize ClientProtocol.
   *
   * @return ClientProtocol
   *
   * @throws IOException
   */
  private static ClientProtocol getClientProtocol(ConfigurationSource config)
      throws IOException {
    return getClientProtocol(config, null);
  }

  /**
   * Returns an instance of Protocol class.
   *
   *
   * @param config
   *        Configuration used to initialize ClientProtocol.
   *
   * @return ClientProtocol
   *
   * @throws IOException
   */
  private static ClientProtocol getClientProtocol(ConfigurationSource config,
      String omServiceId) throws IOException {
    try {
      return new RpcClient(config, omServiceId);
    } catch (Exception e) {
      final String message = "Couldn't create RpcClient protocol";
      LOG.error(message + " exception: ", e);
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(message, e);
      }
    }
  }

}
