/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.proxy;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Proxy provider for
 * {@link org.apache.hadoop.hdds.protocol.SCMSecurityProtocol} against a single
 * SCM node (no fail-over).
 */
public class SingleSCMSecurityProtocolProxyProvider
    extends SCMSecurityProtocolFailoverProxyProvider {
  private final String scmNodeId;

  public SingleSCMSecurityProtocolProxyProvider(
      ConfigurationSource conf,
      UserGroupInformation userGroupInformation,
      String scmNodeId) {
    super(conf, userGroupInformation);
    this.scmNodeId = scmNodeId;
  }

  @Override
  public synchronized String getCurrentProxySCMNodeId() {
    return scmNodeId;
  }

  @Override
  public synchronized void performFailover(SCMSecurityProtocolPB currentProxy) {
    // do nothing.
  }

  @Override
  public synchronized void performFailoverToAssignedLeader(String newLeader,
      Exception e) {
    // do nothing.
  }
}
