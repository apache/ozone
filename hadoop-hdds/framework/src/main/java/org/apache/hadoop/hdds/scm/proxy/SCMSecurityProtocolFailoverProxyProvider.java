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

package org.apache.hadoop.hdds.scm.proxy;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failover proxy provider for SCMSecurityProtocol server.
 */
public class SCMSecurityProtocolFailoverProxyProvider extends SCMFailoverProxyProviderBase<SCMSecurityProtocolPB> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMSecurityProtocolFailoverProxyProvider.class);

  /**
   * Construct fail-over proxy provider for SCMSecurityProtocol Server.
   */
  public SCMSecurityProtocolFailoverProxyProvider(ConfigurationSource conf,
      UserGroupInformation userGroupInformation) {
    super(SCMSecurityProtocolPB.class, conf, userGroupInformation);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected String getProtocolAddress(SCMNodeInfo scmNodeInfo) {
    return scmNodeInfo.getScmSecurityAddress();
  }
}
