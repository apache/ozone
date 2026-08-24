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

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.server.ServiceRuntimeInfoImpl;
import org.apache.hadoop.hdds.utils.VersionInfo;

/**
 * This is the JMX management class for DN information.
 */
public class DNMXBeanImpl extends ServiceRuntimeInfoImpl implements DNMXBean {

  private String hostName;
  private String clientRpcPort;
  private String httpPort;
  private String httpsPort;

  public DNMXBeanImpl(VersionInfo versionInfo) {
    super(versionInfo);
  }

  @Override
  public String getHostname() {
    return hostName;
  }

  @Override
  public String getClientRpcPort() {
    return clientRpcPort;
  }

  @Override
  public String getHttpPort() {
    return httpPort;
  }

  @Override
  public String getHttpsPort() {
    return httpsPort;
  }

  public void setHttpPort(String httpPort) {
    this.httpPort = httpPort;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public void setClientRpcPort(String rpcPort) {
    this.clientRpcPort = rpcPort;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHttpsPort(String httpsPort) {
    this.httpsPort = httpsPort;
  }
}
