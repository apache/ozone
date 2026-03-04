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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;

import java.io.IOException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.ratis.util.function.CheckedConsumer;

/**
 * Reconfiguration handler for Datanode. Allows reconfiguration of some properties
 * even if the properties are not registered in advance.
 */
public class DatanodeReconfigurationHandler extends ReconfigurationHandler {

  private final String scmServiceId;

  public DatanodeReconfigurationHandler(String name, String scmServiceId, OzoneConfiguration config,
      CheckedConsumer<String, IOException> requireAdminPrivilege) {
    super(name, config, requireAdminPrivilege);
    this.scmServiceId = scmServiceId;
  }

  @Override
  public boolean isPropertyReconfigurable(String property) {
    if (scmServiceId != null &&
        property.startsWith(ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY, scmServiceId))) {
      // Allow reconfiguration for "ozone.scm.address.<service>.<node>"
      // even if the property has not been registered. This is to allow
      // SCM migration without datanode restarts.
      return true;
    }
    return getReconfigurableProperties().contains(property);
  }
}
