/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
    private String nameSpace;
    private String hostName;
    private String rpcPort;
    private String httpPort;

    public DNMXBeanImpl(
      VersionInfo versionInfo) {
    super(versionInfo);
  }

    public String getNamespace() {
        return nameSpace;
    }

    @Override
    public String getHostname() {
        return hostName;
    }

    @Override
    public String getRpcPort() {
        return rpcPort;
    }

    @Override
    public String getHttpPort() {
      return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setRpcPort(String rpcPort) {
        this.rpcPort = rpcPort;
    }
}
