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

package org.apache.hadoop.ozone.om.protocolPB;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtocolInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFollowerReadFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneManagerService;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

/**
 * Protocol used to communicate with OM.
 */
@ProtocolInfo(protocolName =
    "org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol",
    protocolVersion = 1)
@KerberosInfo(
    serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(OzoneDelegationTokenSelector.class)
@InterfaceAudience.Private
public interface OzoneManagerProtocolPB
    extends OzoneManagerService.BlockingInterface {
  static OzoneManagerProtocolPB newProxy(OMFailoverProxyProviderBase<OzoneManagerProtocolPB> failoverProxyProvider,
      int maxFailovers) {
    return (OzoneManagerProtocolPB) RetryProxy.create(OzoneManagerProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy(maxFailovers));
  }
  
  static OzoneManagerProtocolPB newProxy(HadoopRpcOMFollowerReadFailoverProxyProvider
      followerReadFailoverProxyProvider, int maxFailovers) {
    return (OzoneManagerProtocolPB) RetryProxy.create(OzoneManagerProtocolPB.class, followerReadFailoverProxyProvider,
        followerReadFailoverProxyProvider.getRetryPolicy(maxFailovers));
  }
}
