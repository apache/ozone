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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_BLOCK_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_CERTIFICATE_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_CONTAINER_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_DATANODE_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_OM_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_SCM_PROTOCOL_ACL;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Unstable;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolDatanode;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolOm;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.protocol.StorageContainerDatanodeProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * {@link PolicyProvider} for SCM protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class SCMPolicyProvider extends PolicyProvider {

  private static final Supplier<SCMPolicyProvider> SUPPLIER =
      MemoizedSupplier.valueOf(SCMPolicyProvider::new);

  private static final List<Service> SCM_SERVICES =
      Arrays.asList(
          new Service(
              HDDS_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL,
              StorageContainerDatanodeProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_CONTAINER_PROTOCOL_ACL,
              StorageContainerLocationProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_BLOCK_PROTOCOL_ACL,
              ScmBlockLocationProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_CERTIFICATE_PROTOCOL_ACL,
              SCMSecurityProtocol.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_OM_PROTOCOL_ACL,
              SecretKeyProtocolOm.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_SCM_PROTOCOL_ACL,
              SecretKeyProtocolScm.class),
          new Service(
              HDDS_SECURITY_CLIENT_SCM_SECRET_KEY_DATANODE_PROTOCOL_ACL,
              SecretKeyProtocolDatanode.class),
          new Service(
              OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL,
              ReconfigureProtocol.class)
      );

  private SCMPolicyProvider() {
  }

  @Private
  @Unstable
  public static SCMPolicyProvider getInstance() {
    return SUPPLIER.get();
  }

  @Override
  public Service[] getServices() {
    return SCM_SERVICES.toArray(new Service[0]);
  }

}
