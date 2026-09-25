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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.server.http.HttpServerConfigurationException;
import org.apache.hadoop.hdds.server.http.TestHttpServer2;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestStorageContainerManager {

  /**
   * A javax filter that cannot be bridged into the Jakarta chain leaves the SCM web server
   * unusable, and such a misconfiguration will never succeed on retry. SCM start-up must abort
   * rather than log the failure and continue serving without a web server.
   */
  @Test
  void startThrowsOnNonBridgeableFilter(@TempDir File dir) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(dir);
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        TestHttpServer2.NonBridgeableFilterInitializer.class.getName());
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());

    StorageContainerManager scm = HddsTestUtils.getScm(conf, configurator);
    try {
      assertThrows(HttpServerConfigurationException.class, scm::start);
    } finally {
      scm.stop();
      scm.join();
    }
  }

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
