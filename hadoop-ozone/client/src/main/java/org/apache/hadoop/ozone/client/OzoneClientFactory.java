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

import java.io.IOException;
import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
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
  public static OzoneClient getClient() throws IOException {
    LOG.info("Creating OzoneClient with default configuration.");
    return getClient(new OzoneConfiguration());
  }

  /**
   * Constructs and return an OzoneClient based on the configuration object.
   * Protocol type is decided by <code>ozone.client.protocol</code>.
   *
   * @param config
   *        Configuration to be used for OzoneClient creation
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getClient(Configuration config)
      throws IOException {
    Preconditions.checkNotNull(config);
    return getClient(getClientProtocol(config), config);
  }

  /**
   * Returns an OzoneClient which will use RPC protocol.
   *
   * @param omHost
   *        hostname of OzoneManager to connect.
   *
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost)
      throws IOException {
    Configuration config = new OzoneConfiguration();
    int port = OmUtils.getOmRpcPort(config);
    return getRpcClient(omHost, port, config);
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
   * @return OzoneClient
   *
   * @throws IOException
   */
  public static OzoneClient getRpcClient(String omHost, Integer omRpcPort)
      throws IOException {
    return getRpcClient(omHost, omRpcPort, new OzoneConfiguration());
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
  public static OzoneClient getRpcClient(String omHost, Integer omRpcPort,
      String omServiceId, Configuration config) throws IOException {
    Preconditions.checkNotNull(omHost);
    Preconditions.checkNotNull(omRpcPort);
    Preconditions.checkNotNull(omServiceId);
    Preconditions.checkNotNull(config);
    config.set(OZONE_OM_ADDRESS_KEY, omHost + ":" + omRpcPort);
    return getRpcClient(omServiceId, config);
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
                                         Configuration config)
      throws IOException {
    Preconditions.checkNotNull(omHost);
    Preconditions.checkNotNull(omRpcPort);
    Preconditions.checkNotNull(config);
    config.set(OZONE_OM_ADDRESS_KEY, omHost + ":" + omRpcPort);
    return getRpcClient(config);
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
      Configuration config) throws IOException {
    Preconditions.checkNotNull(omServiceId);
    Preconditions.checkNotNull(config);
    // Won't set OZONE_OM_ADDRESS_KEY here since service id is passed directly,
    // leaving OZONE_OM_ADDRESS_KEY value as is.
    return getClient(getClientProtocol(config, omServiceId), config);
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
  public static OzoneClient getRpcClient(Configuration config)
      throws IOException {
    Preconditions.checkNotNull(config);
    return getClient(getClientProtocol(config),
        config);
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
  private static OzoneClient getClient(ClientProtocol clientProtocol,
                                       Configuration config) {
    OzoneClientInvocationHandler clientHandler =
        new OzoneClientInvocationHandler(clientProtocol);
    ClientProtocol proxy = (ClientProtocol) Proxy.newProxyInstance(
        OzoneClientInvocationHandler.class.getClassLoader(),
        new Class<?>[]{ClientProtocol.class}, clientHandler);
    return new OzoneClient(config, proxy);
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
  private static ClientProtocol getClientProtocol(Configuration config)
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
  private static ClientProtocol getClientProtocol(Configuration config,
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
