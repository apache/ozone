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
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;

/**
 * 'Aspect' for OM component version API. All methods annotated with the
 * specific annotation will have pre-processing done here to check version compatibility.
 */
@Aspect
public class OMRequestVersionAspect {

  @Before("@annotation(DisallowedUntilLayoutVersion) && execution(* *(..))")
  public void checkLayoutFeature(JoinPoint joinPoint) throws IOException {
    ComponentVersion layoutFeature = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(DisallowedUntilLayoutVersion.class)
        .value();
    checkFeatureAllowed(joinPoint, layoutFeature);
  }

  @Before("@annotation(DisallowedUntilOmVersion) && execution(* *(..))")
  public void checkOmVersion(JoinPoint joinPoint) throws IOException {
    ComponentVersion omVersion = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(DisallowedUntilOmVersion.class)
        .value();
    checkFeatureAllowed(joinPoint, omVersion);
  }

  private void checkFeatureAllowed(JoinPoint joinPoint, ComponentVersion version) throws IOException {
    OMVersionManager versionManager = null;
    final Object[] args = joinPoint.getArgs();
    if (joinPoint.getTarget() instanceof OzoneManagerRequestHandler) {
      OzoneManager ozoneManager = ((OzoneManagerRequestHandler)
          joinPoint.getTarget()).getOzoneManager();
      versionManager = ozoneManager.getVersionManager();
    } else if (joinPoint.getTarget() instanceof OMClientRequest &&
        joinPoint.toShortString().endsWith(".preExecute(..))")) {
      // Get OzoneManager instance from preExecute first argument
      OzoneManager ozoneManager = (OzoneManager) args[0];
      versionManager = ozoneManager.getVersionManager();
    } else {
      throw new IOException(
          "Unable to resolve OMVersionManager for version validation; "
              + "expected OzoneManagerRequestHandler or OMClientRequest.preExecute: "
              + joinPoint.toShortString());
    }
    // Throws an exception that must be propagated if the request is not allowed.
    checkIsAllowed(joinPoint.getSignature().toShortString(), versionManager, version);
  }

  private void checkIsAllowed(String operationName,
                              OMVersionManager omVersionManager,
                              ComponentVersion version) throws OMException {
    if (!omVersionManager.isAllowed(version)) {
      throw new OMException(String.format("Operation %s cannot be invoked " +
              "before finalization. It belongs to version %s. Current apparent version is %s",
          operationName,
          version,
          omVersionManager.getApparentVersion()),
          NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION);
    }
  }

  /**
   * Note: Without this, it occasionally throws NoSuchMethodError when running
   * the test.
   */
  public static OMRequestVersionAspect aspectOf() {
    return new OMRequestVersionAspect();
  }

}
