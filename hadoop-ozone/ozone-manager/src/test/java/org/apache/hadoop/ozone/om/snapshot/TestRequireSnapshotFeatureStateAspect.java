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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.Test;

/**
 * Class to test annotation based interceptor that checks whether
 * Ozone snapshot feature is enabled.
 */
public class TestRequireSnapshotFeatureStateAspect {

  /**
   * Check Aspect implementation with SnapshotFeatureEnabledUtil.
   */
  @Test
  public void testSnapshotFeatureEnabledAnnotation() throws Exception {
    SnapshotFeatureEnabledUtil testObj = new SnapshotFeatureEnabledUtil();
    RequireSnapshotFeatureStateAspect
        aspect = new RequireSnapshotFeatureStateAspect();

    JoinPoint joinPoint = mock(JoinPoint.class);
    when(joinPoint.getTarget()).thenReturn(testObj);

    MethodSignature methodSignature = mock(MethodSignature.class);
    when(methodSignature.getMethod()).thenReturn(
        SnapshotFeatureEnabledUtil.class.getMethod("snapshotMethod"));
    when(methodSignature.toShortString()).thenReturn("snapshotMethod");
    when(joinPoint.getSignature()).thenReturn(methodSignature);

    OMException omException = assertThrows(OMException.class,
        () -> aspect.checkFeatureState(joinPoint));
    assertEquals("Operation snapshotMethod cannot be invoked " +
            "because Ozone snapshot feature is disabled.",
        omException.getMessage());
  }
}
