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

package org.apache.hadoop.hdds.scm.safemode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;

class TestSafeModeRuleFactory {

  @Test
  public void testIllegalState() {
    resetInstance();
    assertThrows(IllegalStateException.class, SafeModeRuleFactory::getInstance);
  }

  @Test
  public void testLoadedSafeModeRules() {
    resetInstance();
    SCMSafeModeManager safeModeManager = initializeSafeModeRuleFactory();
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();
    factory.addSafeModeManager(safeModeManager);

    // Currently we assert the total count against hardcoded value
    // as the rules are hardcoded in SafeModeRuleFactory.

    // This will be fixed once we load rules using annotation.
    assertEquals(6, factory.getSafeModeRules().size(),
        "The total safemode rules count doesn't match");

  }

  @Test
  public void testLoadedPreCheckRules() {
    resetInstance();
    SCMSafeModeManager safeModeManager = initializeSafeModeRuleFactory();
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();
    factory.addSafeModeManager(safeModeManager);

    // Currently we assert the total count against hardcoded value
    // as the rules are hardcoded in SafeModeRuleFactory.

    // This will be fixed once we load rules using annotation.
    assertEquals(1, factory.getPreCheckRules().size(),
        "The total safemode rules count doesn't match");

  }

  @Test
  public void testRuleCountForEcDefaultWithRatisThreeFlagDisabled() {
    resetInstance();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.EC.name());
    conf.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024k");
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATE_RATIS_THREE,
        false);

    SCMSafeModeManager safeModeManager = initializeSafeModeRuleFactory(conf);
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();
    factory.addSafeModeManager(safeModeManager);

    assertEquals(4, factory.getSafeModeRules().size(),
        "EC default with flag=false should skip RATIS/THREE pipeline rules");
  }

  @Test
  public void testRuleCountForEcDefaultWithRatisThreeFlagEnabled() {
    resetInstance();
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.EC.name());
    conf.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024k");
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATE_RATIS_THREE,
        true);

    SCMSafeModeManager safeModeManager = initializeSafeModeRuleFactory(conf);
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();
    factory.addSafeModeManager(safeModeManager);

    assertEquals(6, factory.getSafeModeRules().size(),
        "EC default with flag=true should include RATIS/THREE pipeline rules");
  }

  private SCMSafeModeManager initializeSafeModeRuleFactory() {
    return initializeSafeModeRuleFactory(new OzoneConfiguration());
  }

  private SCMSafeModeManager initializeSafeModeRuleFactory(
      OzoneConfiguration configuration) {
    final SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    when(safeModeManager.getSafeModeMetrics()).thenReturn(mock(SafeModeMetrics.class));
    configuration.set(HddsConfigKeys.HDDS_SCM_SAFEMODE_RULE_REFRESH_INTERVAL,
        "0s");
    SafeModeRuleFactory.initialize(configuration,
        SCMContext.emptyContext(), new EventQueue(), mock(
            PipelineManager.class),
        mock(ContainerManager.class), mock(NodeManager.class));
    return safeModeManager;
  }

  private static void resetInstance() {
    try {
      final Field instance = SafeModeRuleFactory.class.getDeclaredField(
          "instance");
      instance.setAccessible(true);
      instance.set(null, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
