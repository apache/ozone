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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.net.StaticMapping;
import org.junit.jupiter.api.Test;

class TestStorageContainerManager {

  @Test
  void defaultMappingKeepsCachedBehavior() {
    DNSToSwitchMapping mapping =
        StorageContainerManager.createDNSToSwitchMapping(
            new OzoneConfiguration());

    assertInstanceOf(ScriptBasedMapping.class, mapping);
    assertInstanceOf(CachedDNSToSwitchMapping.class, mapping);
  }

  @Test
  void configuredMappingIsUsedDirectly() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setClass(ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);

    DNSToSwitchMapping mapping =
        StorageContainerManager.createDNSToSwitchMapping(conf);

    assertInstanceOf(StaticMapping.class, mapping);
    assertFalse(mapping instanceof CachedDNSToSwitchMapping,
        "Configured mapping should not be wrapped in CachedDNSToSwitchMapping");
  }
}
