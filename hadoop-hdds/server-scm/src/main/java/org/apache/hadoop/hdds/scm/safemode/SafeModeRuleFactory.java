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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;

/**
 * Factory to create SafeMode rules.
 */
public final class SafeModeRuleFactory {


  private final ConfigurationSource config;
  private final SCMContext scmContext;
  private final EventQueue eventQueue;

  // TODO: Remove dependency on safeModeManager (HDDS-11797)
  private final SCMSafeModeManager safeModeManager;
  private final PipelineManager pipelineManager;
  private final ContainerManager containerManager;
  private final NodeManager nodeManager;

  private final List<SafeModeExitRule<?>> safeModeRules;
  private final List<SafeModeExitRule<?>> preCheckRules;

  private static SafeModeRuleFactory instance;

  private SafeModeRuleFactory(final ConfigurationSource config,
                              final SCMContext scmContext,
                              final EventQueue eventQueue,
                              final SCMSafeModeManager safeModeManager,
                              final PipelineManager pipelineManager,
                              final ContainerManager containerManager,
                              final NodeManager nodeManager) {
    this.config = config;
    this.scmContext = scmContext;
    this.eventQueue = eventQueue;
    this.safeModeManager = safeModeManager;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
    this.safeModeRules = new ArrayList<>();
    this.preCheckRules = new ArrayList<>();
    loadRules();
  }

  private void loadRules() {
    // TODO: Use annotation to load the rules. (HDDS-11730)
    SafeModeExitRule<?> containerRule = new ContainerSafeModeRule(eventQueue, 
        config, containerManager, safeModeManager);
    SafeModeExitRule<?> datanodeRule = new DataNodeSafeModeRule(eventQueue, 
        config, nodeManager, safeModeManager);

    safeModeRules.add(containerRule);
    safeModeRules.add(datanodeRule);

    preCheckRules.add(datanodeRule);

    if (pipelineManager != null) {
      safeModeRules.add(new HealthyPipelineSafeModeRule(eventQueue, pipelineManager,
          safeModeManager, config, scmContext));
      safeModeRules.add(new OneReplicaPipelineSafeModeRule(eventQueue, pipelineManager,
          safeModeManager, config));
    }

  }

  public static synchronized SafeModeRuleFactory getInstance() {
    if (instance != null) {
      return instance;
    }
    throw new IllegalStateException("SafeModeRuleFactory not initialized," +
        " call initialize method before getInstance.");
  }

  // TODO: Refactor and reduce the arguments. (HDDS-11800)
  public static synchronized void initialize(
      final ConfigurationSource config,
      final SCMContext scmContext,
      final EventQueue eventQueue,
      final SCMSafeModeManager safeModeManager,
      final PipelineManager pipelineManager,
      final ContainerManager containerManager,
      final NodeManager nodeManager) {
    instance = new SafeModeRuleFactory(config, scmContext, eventQueue,
          safeModeManager, pipelineManager, containerManager, nodeManager);
  }

  public List<SafeModeExitRule<?>> getSafeModeRules() {
    return safeModeRules;
  }

  public List<SafeModeExitRule<?>> getPreCheckRules() {
    return preCheckRules;
  }
}
