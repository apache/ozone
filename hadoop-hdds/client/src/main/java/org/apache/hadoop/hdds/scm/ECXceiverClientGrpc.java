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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_EC_GRPC_RETRIES_MAX_DEFAULT;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;

/**
 * {@link XceiverClientSpi} implementation to work specifically with EC
 * related requests. The only difference at the moment from the basic
 * {@link XceiverClientGrpc} is that this implementation does async calls when
 * a write request is posted via the sendCommandAsync method.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HDDS-5954">HDDS-5954</a>
 */
public class ECXceiverClientGrpc extends XceiverClientGrpc {

  private final boolean enableRetries;

  public ECXceiverClientGrpc(
      Pipeline pipeline,
      ConfigurationSource config,
      ClientTrustManager trustManager) {
    super(pipeline, config, trustManager);
    this.enableRetries = config.getBoolean(OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED,
        OZONE_CLIENT_EC_GRPC_RETRIES_ENABLED_DEFAULT);
    setTimeout(config.getTimeDuration(OzoneConfigKeys.
        OZONE_CLIENT_EC_GRPC_WRITE_TIMEOUT, OzoneConfigKeys
        .OZONE_CLIENT_EC_GRPC_WRITE_TIMEOUT_DEFAULT, TimeUnit.SECONDS));
  }

  /**
   * For EC writes, due to outside syncronization points during writes, it is
   * not necessary to block any async requests that are
   * arriving via the
   * {@link #sendCommandAsync(ContainerProtos.ContainerCommandRequestProto)}
   * method.
   *
   * @param request the request we need the decision about
   * @return false always to do not block async requests.
   */
  @Override
  protected boolean shouldBlockAndWaitAsyncReply(
      ContainerProtos.ContainerCommandRequestProto request) {
    return false;
  }

  @Override
  protected NettyChannelBuilder createChannel(DatanodeDetails dn, int port)
      throws IOException {
    NettyChannelBuilder channelBuilder = super.createChannel(dn, port);
    if (enableRetries) {
      double maxAttempts = getConfig().getInt(OZONE_CLIENT_EC_GRPC_RETRIES_MAX,
          OZONE_CLIENT_EC_GRPC_RETRIES_MAX_DEFAULT);

      channelBuilder.defaultServiceConfig(createRetryServiceConfig(maxAttempts))
          .maxRetryAttempts((int) maxAttempts).enableRetry();
    }
    return channelBuilder;
  }

  private Map<String, Object> createRetryServiceConfig(double maxAttempts) {
    Map<String, Object> retryPolicy = new HashMap<>();
    // Maximum number of RPC attempts which includes the original RPC.
    retryPolicy.put("maxAttempts", maxAttempts);
    // The initial retry attempt will occur at random(0, initialBackoff)
    retryPolicy.put("initialBackoff", "0.5s");
    retryPolicy.put("maxBackoff", "3s");
    retryPolicy.put("backoffMultiplier", 1.5D);
    //Status codes for with RPC retry are attempted.
    retryPolicy.put("retryableStatusCodes", Collections.singletonList(
        Status.Code.DEADLINE_EXCEEDED.name()));
    Map<String, Object> methodConfig = new HashMap<>();
    methodConfig.put("retryPolicy", retryPolicy);

    Map<String, Object> name = new HashMap<>();
    name.put("service", "hadoop.hdds.datanode.XceiverClientProtocolService");
    methodConfig.put("name", Collections.singletonList(name));

    Map<String, Object> serviceConfig = new HashMap<>();
    serviceConfig.put("methodConfig",
        Collections.singletonList(methodConfig));
    return serviceConfig;
  }
}
