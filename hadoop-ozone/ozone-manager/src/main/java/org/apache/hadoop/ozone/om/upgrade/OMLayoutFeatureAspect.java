/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;


/**
 * 'Aspect' for OM Layout Feature API. All methods annotated with the
 * specific annotation will have pre-processing done here to check layout
 * version compatibility.
 */
@Aspect
public class OMLayoutFeatureAspect {

  @Before("@annotation(DisallowedUntilLayoutVersion) && execution(* *(..))")
  public void checkLayoutFeature(JoinPoint joinPoint) throws Throwable {
    String featureName = ((MethodSignature) joinPoint.getSignature())
        .getMethod().getAnnotation(DisallowedUntilLayoutVersion.class)
        .value().name();
    LayoutVersionManager lvm = OMLayoutVersionManager.getInstance();
    if (!lvm.isAllowed(featureName)) {
      LayoutFeature layoutFeature = lvm.getFeature(featureName);
      throw new OMException(String.format("Operation %s cannot be invoked " +
              "before finalization. It belongs to the layout feature %s, " +
              "whose layout version is %d. Current Layout version is %d",
          joinPoint.getSignature().toShortString(),
          layoutFeature.name(),
          layoutFeature.layoutVersion(),
          lvm.getMetadataLayoutVersion()),
          NOT_SUPPORTED_OPERATION);
    }
  }
}
