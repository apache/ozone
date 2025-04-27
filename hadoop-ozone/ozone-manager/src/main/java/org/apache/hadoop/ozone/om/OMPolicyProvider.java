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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SECURITY_ADMIN_PROTOCOL_ACL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Unstable;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.ozone.om.protocol.OMAdminProtocol;
import org.apache.hadoop.ozone.om.protocol.OMInterServiceProtocol;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * {@link PolicyProvider} for OM protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class OMPolicyProvider extends PolicyProvider {

  private static final Supplier<OMPolicyProvider> SUPPLIER =
      MemoizedSupplier.valueOf(OMPolicyProvider::new);

  private static final List<Service> OM_SERVICES =
      Arrays.asList(
          new Service(OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL,
              OzoneManagerProtocol.class),
          new Service(OZONE_OM_SECURITY_ADMIN_PROTOCOL_ACL,
              OMInterServiceProtocol.class),
          new Service(OZONE_OM_SECURITY_ADMIN_PROTOCOL_ACL,
              OMAdminProtocol.class),
          new Service(OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL,
              ReconfigureProtocol.class)
      );

  private OMPolicyProvider() {
  }

  @Private
  @Unstable
  public static OMPolicyProvider getInstance() {
    return SUPPLIER.get();
  }

  @Override
  public Service[] getServices() {
    return OM_SERVICES.toArray(new Service[0]);
  }

}
