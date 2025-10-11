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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;

import java.io.IOException;
import java.lang.reflect.Method;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 'Aspect' for OM Layout Feature API. All methods annotated with the
 * specific annotation will have pre-processing done here to check layout
 * version compatibility.
 */
@Aspect
public class OMLayoutFeatureAspect {

  public static final String GET_VERSION_MANAGER_METHOD_NAME =
      "getOmVersionManager";
  private static final Logger LOG = LoggerFactory
      .getLogger(OMLayoutFeatureAspect.class);

  @Before("@annotation(DisallowedUntilLayoutVersion) && execution(* *(..))")
  public void checkLayoutFeature(JoinPoint joinPoint) throws IOException {
    String featureName = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(DisallowedUntilLayoutVersion.class)
        .value().name();
    LayoutVersionManager lvm;
    final Object[] args = joinPoint.getArgs();
    if (joinPoint.getTarget() instanceof OzoneManagerRequestHandler) {
      OzoneManager ozoneManager = ((OzoneManagerRequestHandler)
          joinPoint.getTarget()).getOzoneManager();
      lvm = ozoneManager.getVersionManager();
    } else if (joinPoint.getTarget() instanceof OMClientRequest &&
        joinPoint.toShortString().endsWith(".preExecute(..))")) {
      // Get OzoneManager instance from preExecute first argument
      OzoneManager ozoneManager = (OzoneManager) args[0];
      lvm = ozoneManager.getVersionManager();
    } else {
      try {
        Method method = joinPoint.getTarget().getClass()
            .getMethod(GET_VERSION_MANAGER_METHOD_NAME);
        lvm = (LayoutVersionManager) method.invoke(joinPoint.getTarget());
      } catch (Exception ex) {
        lvm = new OMLayoutVersionManager();
      }
    }
    checkIsAllowed(joinPoint.getSignature().toShortString(), lvm, featureName);
  }

  private void checkIsAllowed(String operationName,
                              LayoutVersionManager lvm,
                              String featureName) throws OMException {
    if (!lvm.isAllowed(featureName)) {
      LayoutFeature layoutFeature = lvm.getFeature(featureName);
      throw new OMException(String.format("Operation %s cannot be invoked " +
              "before finalization. It belongs to the layout feature %s, " +
              "whose layout version is %d. Current Layout version is %d",
          operationName,
          layoutFeature.name(),
          layoutFeature.layoutVersion(),
          lvm.getMetadataLayoutVersion()),
          NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
  }

  @Pointcut("execution(* " +
      "org.apache.hadoop.ozone.om.request.OMClientRequest+.preExecute(..)) " +
      "&& @this(org.apache.hadoop.ozone.om.upgrade.BelongsToLayoutVersion)")
  public void omRequestPointCut() {
  }

  @Before("omRequestPointCut()")
  public void beforeRequestApplyTxn(final JoinPoint joinPoint)
      throws OMException {

    BelongsToLayoutVersion annotation = joinPoint.getTarget().getClass()
        .getAnnotation(BelongsToLayoutVersion.class);
    if (annotation == null) {
      return;
    }

    Object[] args = joinPoint.getArgs();
    OzoneManager om = (OzoneManager) args[0];

    LayoutFeature lf = annotation.value();
    checkIsAllowed(joinPoint.getTarget().getClass().getSimpleName(),
        om.getVersionManager(), lf.name());
  }

  /**
   * Note: Without this, it occasionally throws NoSuchMethodError when running
   * the test.
   */
  public static OMLayoutFeatureAspect aspectOf() {
    return new OMLayoutFeatureAspect();
  }

}
