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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED_DEFAULT;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StorageContainerManager enters safe mode on startup to allow system to
 * reach a stable state before becoming fully functional. SCM will wait
 * for certain resources to be reported before coming out of safe mode.<p>
 *
 * Set of {@link SafeModeExitRule} are defined to verify if the required
 * resources are reported, so that SCM can come out of safemode.<p>
 *
 * There are two stages in safemode exit,
 * <ul>
 *   <li>pre-check complete</li>
 *   <li>safemode exit</li>
 * </ul>
 * <br>
 * Each {@link SafeModeExitRule} can be configured to be part of either
 * {@code pre-check}, {@code safemode} or both.<p>
 *
 * <i>Note: The Safemode logic can be completely disabled using
 * {@link org.apache.hadoop.hdds.HddsConfigKeys#HDDS_SCM_SAFEMODE_ENABLED} property</i>
 * <p>
 *
 * @see SafeModeExitRule
 * @see DataNodeSafeModeRule
 * @see HealthyPipelineSafeModeRule
 * @see OneReplicaPipelineSafeModeRule
 * @see RatisContainerSafeModeRule
 * @see ECContainerSafeModeRule
 */
public class SCMSafeModeManager implements SafeModeManager {

  private static final Logger LOG = LoggerFactory.getLogger(SCMSafeModeManager.class);

  private final AtomicBoolean inSafeMode = new AtomicBoolean(true);
  private final AtomicBoolean preCheckComplete = new AtomicBoolean(false);
  private final Map<String, SafeModeExitRule<?>> exitRules = new HashMap<>();
  private final Set<String> preCheckRules = new HashSet<>();
  private final Set<String> validatedRules = new HashSet<>();
  private final Set<String> validatedPreCheckRules = new HashSet<>();

  private final SCMServiceManager serviceManager;
  private final SCMContext scmContext;
  private final SafeModeMetrics safeModeMetrics;

  public SCMSafeModeManager(final ConfigurationSource conf,
                            final NodeManager nodeManager,
                            final PipelineManager pipelineManager,
                            final ContainerManager containerManager,
                            final SCMServiceManager serviceManager,
                            final EventQueue eventQueue,
                            final  SCMContext scmContext) {
    this.serviceManager = serviceManager;
    this.scmContext = scmContext;
    this.safeModeMetrics = SafeModeMetrics.create();

    // TODO: Remove the cyclic ("this") dependency (HDDS-11797)
    SafeModeRuleFactory.initialize(conf, scmContext, eventQueue, this,
        pipelineManager, containerManager, nodeManager);
    SafeModeRuleFactory factory = SafeModeRuleFactory.getInstance();
    factory.getSafeModeRules().forEach(rule -> exitRules.put(rule.getRuleName(), rule));
    factory.getPreCheckRules().forEach(rule -> preCheckRules.add(rule.getRuleName()));

    final boolean isSafeModeEnabled = conf.getBoolean(HDDS_SCM_SAFEMODE_ENABLED, HDDS_SCM_SAFEMODE_ENABLED_DEFAULT);
    if (!isSafeModeEnabled) {
      LOG.info("Safemode is disabled, skipping Safemode rule validation and force exiting Safemode.");
      exitSafeMode();
    }
  }

  public void start() {
    emitSafeModeStatus();
  }

  public void stop() {
    safeModeMetrics.unRegister();
  }

  public SafeModeMetrics getSafeModeMetrics() {
    return safeModeMetrics;
  }

  private void emitSafeModeStatus() {
    final SafeModeStatus safeModeStatus = SafeModeStatus.of(
        getInSafeMode(), getPreCheckComplete());
    scmContext.updateSafeModeStatus(safeModeStatus);

    // notify SCMServiceManager
    if (!safeModeStatus.isInSafeMode()) {
      // If safemode is off, then notify the delayed listeners with a delay.
      serviceManager.notifyStatusChanged();
    } else if (safeModeStatus.isPreCheckComplete()) {
      // Only notify the delayed listeners if safemode remains on, as precheck
      // may have completed.
      serviceManager.notifyEventTriggered(Event.PRE_CHECK_COMPLETED);
    }
  }

  public synchronized void validateSafeModeExitRules(String ruleName) {
    if (exitRules.containsKey(ruleName)) {
      validatedRules.add(ruleName);
      LOG.info("{} rule is successfully validated", ruleName);
      if (preCheckRules.contains(ruleName)) {
        validatedPreCheckRules.add(ruleName);
      }
    } else {
      // This should never happen
      LOG.error("No Such Exit rule {}", ruleName);
    }

    if (!getPreCheckComplete()) {
      completePreCheck();
    }

    if (validatedRules.size() == exitRules.size()) {
      // All rules are satisfied, we can exit safe mode.
      LOG.info("ScmSafeModeManager, all rules are successfully validated");
      exitSafeMode();
    }
  }

  /**
   * When all the precheck rules have been validated, set preCheckComplete to
   * true and then emit the safemode status so any listeners get notified of
   * the safemode state change.
   */
  private void completePreCheck() {
    if (validatedPreCheckRules.size() == preCheckRules.size()) {
      LOG.info("All SCM safe mode pre check rules have passed");
      preCheckComplete.set(true);
      emitSafeModeStatus();
    }
  }

  public void forceExitSafeMode() {
    exitSafeMode();
  }

  private void exitSafeMode() {
    LOG.info("SCM exiting safe mode.");
    // If safemode is exiting, then pre-check must also have passed.
    preCheckComplete.set(true);
    inSafeMode.set(false);

    // TODO: Remove handler registration as there is no need to listen to
    //   register events anymore.

    emitSafeModeStatus();
  }

  /**
   * Refresh Rule state.
   */
  public void refresh() {
    if (inSafeMode.get()) {
      exitRules.values().forEach(rule -> {
        // Refresh rule irrespective of validate(), as at this point validate
        // does not represent current state validation, as validate is being
        // done with stale state.
        rule.refresh(true);
      });
    }
  }

  /**
   * Refresh Rule state and validate rules.
   */
  public void refreshAndValidate() {
    if (inSafeMode.get()) {
      exitRules.values().forEach(rule -> {
        rule.refresh(false);
        if (rule.validate() && inSafeMode.get()) {
          validateSafeModeExitRules(rule.getRuleName());
          rule.cleanup();
        }
      });
    }
  }

  @Override
  public boolean getInSafeMode() {
    return inSafeMode.get();
  }

  /** Get the safe mode status of all rules. */
  public Map<String, Pair<Boolean, String>> getRuleStatus() {
    Map<String, Pair<Boolean, String>> map = new HashMap<>();
    for (SafeModeExitRule<?> exitRule : exitRules.values()) {
      map.put(exitRule.getRuleName(),
          Pair.of(exitRule.validate(), exitRule.getStatusText()));
    }
    return map;
  }

  public boolean getPreCheckComplete() {
    return preCheckComplete.get();
  }

  public static Logger getLogger() {
    return LOG;
  }

  /**
   * Class used during SafeMode status event.
   */
  public static final class SafeModeStatus {

    private final boolean safeModeStatus;
    private final boolean preCheckPassed;

    private SafeModeStatus(boolean safeModeState, boolean preCheckPassed) {
      this.safeModeStatus = safeModeState;
      this.preCheckPassed = preCheckPassed;
    }

    public static SafeModeStatus of(boolean safeMode, boolean preCheck) {
      return new SafeModeStatus(safeMode, preCheck);
    }

    public boolean isInSafeMode() {
      return safeModeStatus;
    }

    public boolean isPreCheckComplete() {
      return preCheckPassed;
    }

    @Override
    public String toString() {
      return "SafeModeStatus{" +
          "safeModeStatus=" + safeModeStatus +
          ", preCheckPassed=" + preCheckPassed +
          '}';
    }
  }

}
