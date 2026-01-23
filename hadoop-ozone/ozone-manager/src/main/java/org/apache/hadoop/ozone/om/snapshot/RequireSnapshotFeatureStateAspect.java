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

package org.apache.hadoop.ozone.om.snapshot;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;

import java.io.IOException;
import java.lang.reflect.Method;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 'Aspect' for checking whether snapshot feature is enabled.
 * All methods annotated with the specific annotation will have pre-processing
 * done here to check layout version compatibility.
 * Note: Append class to
 *  hadoop-ozone/ozone-manager/src/main/resources/META-INF/aop.xml
 * if the annotation doesn't seem to take affect on other classes' methods.
 */
@Aspect
public class RequireSnapshotFeatureStateAspect {

  private static final Logger LOG = LoggerFactory
      .getLogger(RequireSnapshotFeatureStateAspect.class);

  @Before("@annotation(RequireSnapshotFeatureState) && execution(* *(..))")
  public void checkFeatureState(JoinPoint joinPoint) throws IOException {
    boolean desiredFeatureState = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(RequireSnapshotFeatureState.class)
        .value();
    boolean isFeatureEnabled;
    final Object[] args = joinPoint.getArgs();
    if (LOG.isDebugEnabled()) {
      LOG.debug("joinPoint.getTarget() = {}", joinPoint.getTarget());
    }
    if (joinPoint.getTarget() instanceof OzoneManagerRequestHandler) {
      OzoneManager ozoneManager = ((OzoneManagerRequestHandler)
          joinPoint.getTarget()).getOzoneManager();
      isFeatureEnabled = ozoneManager.isFilesystemSnapshotEnabled();
    } else if (joinPoint.getTarget() instanceof OMClientRequest &&
        joinPoint.toShortString().endsWith(".preExecute(..))")) {
      // Get OzoneManager instance from preExecute first argument
      OzoneManager ozoneManager = (OzoneManager) args[0];
      isFeatureEnabled = ozoneManager.isFilesystemSnapshotEnabled();
    } else {
      // This case is used in UT, where:
      // joinPoint.getTarget() instanceof SnapshotFeatureEnabledUtil
      try {
        Method method = joinPoint.getTarget().getClass()
            .getMethod("isFilesystemSnapshotEnabled");
        isFeatureEnabled = (boolean) method.invoke(joinPoint.getTarget());
      } catch (Exception ex) {
        // Exception being thrown here means this is not called from the UT.
        // Thus this is an unhandled usage.
        // Add more case handling logic as needed.
        throw new NotImplementedException(
            "Unhandled use case. Please implement.");
      }
    }
    checkIsAllowed(joinPoint.getSignature().toShortString(),
        isFeatureEnabled, desiredFeatureState);
  }

  private void checkIsAllowed(String operationName,
                              boolean isFeatureEnabled,
                              boolean desiredFeatureState) throws OMException {

    if (desiredFeatureState) {
      if (!isFeatureEnabled) {
        throw new OMException(String.format(
            "Operation %s cannot be invoked because %s.",
            operationName,
            "Ozone snapshot feature is disabled"),
            FEATURE_NOT_ENABLED);
      } else {
        // Pass the check: feature is enabled, desired feature state is enabled
        return;
      }
    } else {
      // Add implementation if needed later
      // desiredFeatureState=true is the only case being used right now
      throw new NotImplementedException("Check not implemented for case: " +
          "isFeatureEnabled=" + isFeatureEnabled +
          ", desiredFeatureState=" + desiredFeatureState);
    }
  }

  /**
   * Note: Without this, it occasionally throws NoSuchMethodError when running
   * the test.
   */
  public static RequireSnapshotFeatureStateAspect aspectOf() {
    return new RequireSnapshotFeatureStateAspect();
  }

}
