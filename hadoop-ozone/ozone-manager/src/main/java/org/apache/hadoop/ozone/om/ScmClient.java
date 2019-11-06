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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdds.utils.RetriableTask;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForBlockClients;
import static org.apache.hadoop.hdds.HddsUtils.getScmAddressForClients;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep;

/**
 * Wrapper class for Scm protocol calls.
 */
public class ScmClient {
  private OzoneConfiguration config;
  private ScmBlockLocationProtocol blockClient;
  private StorageContainerLocationProtocol containerClient;

  public ScmClient(OzoneConfiguration config) {
    this.config = config;
  }

  private synchronized void initContainerClient() throws IOException {
    if (containerClient == null){
      Class<StorageContainerLocationProtocolPB> protocol =
          StorageContainerLocationProtocolPB.class;

      RPC.setProtocolEngine(config, protocol, ProtobufRpcEngine.class);
      long scmVersion = RPC.getProtocolVersion(protocol);
      InetSocketAddress scmAddr = getScmAddressForClients(config);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      StorageContainerLocationProtocolPB proxy =
          RPC.getProxy(protocol, scmVersion, scmAddr, ugi, config,
              NetUtils.getDefaultSocketFactory(config),
              Client.getRpcTimeout(config));

      containerClient = TracingUtil.createProxy(
              new StorageContainerLocationProtocolClientSideTranslatorPB(proxy),
              StorageContainerLocationProtocol.class, config);
    }
  }

  private synchronized void initBlockClient() throws IOException {
    if (blockClient == null) {
      Class<ScmBlockLocationProtocolPB> protocol =
          ScmBlockLocationProtocolPB.class;

      RPC.setProtocolEngine(config, protocol, ProtobufRpcEngine.class);
      long scmVersion = RPC.getProtocolVersion(protocol);
      InetSocketAddress scmBlockAddress = getScmAddressForBlockClients(config);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      ScmBlockLocationProtocolPB proxy =
          RPC.getProxy(protocol, scmVersion, scmBlockAddress, ugi, config,
              NetUtils.getDefaultSocketFactory(config),
              Client.getRpcTimeout(config)
          );

      blockClient = TracingUtil
          .createProxy(
              new ScmBlockLocationProtocolClientSideTranslatorPB(proxy),
              ScmBlockLocationProtocol.class, config);
    }
  }

  public ScmBlockLocationProtocol getBlockClient() throws IOException {
    initBlockClient();
    return this.blockClient;
  }

  StorageContainerLocationProtocol getContainerClient() throws IOException {
    initContainerClient();
    return this.containerClient;
  }


  public ScmInfo getScmInfo() throws IOException {
    try {
      RetryPolicy retryPolicy = retryUpToMaximumCountWithFixedSleep(
          10, 5, TimeUnit.SECONDS);
      RetriableTask<ScmInfo> retriable = new RetriableTask<>(
          retryPolicy, "OM#getScmInfo", () -> getBlockClient().getScmInfo());
      return retriable.call();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to get SCM info", e);
    }
  }
}
