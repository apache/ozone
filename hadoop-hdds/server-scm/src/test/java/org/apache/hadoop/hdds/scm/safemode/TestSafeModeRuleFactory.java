/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSafeModeRuleFactory {

  @Test
  public void testIllegalState() {
    // If the initialization is already done by different test, we have to reset it.
    try {
      final Field instance = SafeModeRuleFactory.class.getDeclaredField("instance");
      instance.setAccessible(true);
      instance.set(null, null);
    } catch (Exception e) {
      throw new RuntimeException();
    }
    assertThrows(IllegalStateException.class, SafeModeRuleFactory::getInstance);
  }

  @Test
  public void testLoadedSafeModeRules() {
    initializeSafeModeRuleFactory();
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();

    // Currently we assert the total count against hardcoded value
    // as the rules are hardcoded in SafeModeRuleFactory.

    // This will be fixed once we load rules using annotation.
    assertEquals(4, factory.getSafeModeRules().size(),
        "The total safemode rules count doesn't match");

  }

  @Test
  public void testLoadedPreCheckRules() {
    initializeSafeModeRuleFactory();
    final SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();

    // Currently we assert the total count against hardcoded value
    // as the rules are hardcoded in SafeModeRuleFactory.

    // This will be fixed once we load rules using annotation.
    assertEquals(1, factory.getPreCheckRules().size(),
        "The total safemode rules count doesn't match");

  }

  private void initializeSafeModeRuleFactory() {
    final SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    when(safeModeManager.getSafeModeMetrics()).thenReturn(mock(SafeModeMetrics.class));
    SafeModeRuleFactory.initialize(new OzoneConfiguration(),
        SCMContext.emptyContext(), new EventQueue(), safeModeManager, mock(
            PipelineManager.class), mock(ContainerManager.class));
  }

}
