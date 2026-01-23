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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.ipc_.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used from a datanode to Recon. This extends
 * the Protocol Buffers service interface to add Hadoop-specific annotations.
 */
@ProtocolInfo(protocolName =
    "org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol",
    protocolVersion = 1)
@KerberosInfo(
    serverPrincipal = OZONE_RECON_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY)
public interface ReconDatanodeProtocolPB extends
    StorageContainerDatanodeProtocolPB {
}
