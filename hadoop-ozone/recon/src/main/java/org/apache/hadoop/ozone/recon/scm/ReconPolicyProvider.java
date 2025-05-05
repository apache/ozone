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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * {@link PolicyProvider} for Recon protocols.
 */
public final class ReconPolicyProvider extends PolicyProvider {

  private static final Supplier<ReconPolicyProvider> SUPPLIER =
      MemoizedSupplier.valueOf(ReconPolicyProvider::new);

  private static final List<Service> RECON_SERVICES =
      Collections.singletonList(
          new Service(
              OZONE_RECON_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL,
              ReconDatanodeProtocol.class)
      );

  private ReconPolicyProvider() {
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static ReconPolicyProvider getInstance() {
    return SUPPLIER.get();
  }

  @Override
  public Service[] getServices() {
    return RECON_SERVICES.toArray(new Service[0]);
  }

}
