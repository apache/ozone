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

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.annotation.InterfaceAudience.Private;
import org.apache.hadoop.hdds.annotation.InterfaceStability.Unstable;
import org.apache.hadoop.hdds.protocol.ReconfigureProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.ratis.util.MemoizedSupplier;

/**
 * {@link PolicyProvider} for Datanode protocols.
 */
@Private
@Unstable
public final class HddsPolicyProvider extends PolicyProvider {

  private static final Supplier<HddsPolicyProvider> SUPPLIER =
      MemoizedSupplier.valueOf(HddsPolicyProvider::new);

  private static final List<Service> DN_SERVICES =
      Collections.singletonList(
          new Service(
              OZONE_SECURITY_RECONFIGURE_PROTOCOL_ACL,
              ReconfigureProtocol.class)
      );

  private HddsPolicyProvider() {
  }

  @Private
  @Unstable
  public static HddsPolicyProvider getInstance() {
    return SUPPLIER.get();
  }

  @Override
  public Service[] getServices() {
    return DN_SERVICES.toArray(new Service[0]);
  }
}
