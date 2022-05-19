/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ozone.test.LambdaTestUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Class to test annotation based interceptor that checks whether layout
 * feature API is allowed.
 */
public class TestOMLayoutFeatureAspect {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration configuration = new OzoneConfiguration();

  @Before
  public void setUp() throws IOException {
    configuration.set("ozone.metadata.dirs",
        temporaryFolder.newFolder().getAbsolutePath());
  }

  /**
   * This unit test invokes the above 2 layout feature APIs. The first one
   * should fail, and the second one should pass.
   * @throws Exception
   */
  @Test
  public void testDisallowedUntilLayoutVersion() throws Throwable {
    OMLayoutFeatureUtil testObj = new OMLayoutFeatureUtil();
    OMLayoutFeatureAspect aspect = new OMLayoutFeatureAspect();

    JoinPoint joinPoint = mock(JoinPoint.class);
    when(joinPoint.getTarget()).thenReturn(testObj);

    MethodSignature methodSignature = mock(MethodSignature.class);
    when(methodSignature.getMethod())
        .thenReturn(OMLayoutFeatureUtil.class.getMethod("ecMethod"));
    when(joinPoint.getSignature()).thenReturn(methodSignature);

    LambdaTestUtils.intercept(OMException.class,
        "cannot be invoked before finalization",
        () -> aspect.checkLayoutFeature(joinPoint));
  }

  @Test
  public void testPreExecuteLayoutCheck() throws Exception {

    OzoneManager om = mock(OzoneManager.class);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(false);
    when(lvm.getFeature(anyString())).thenReturn(INITIAL_VERSION);
    when(om.getVersionManager()).thenReturn(lvm);

    MockOmRequest mockOmRequest = new MockOmRequest();
    OMLayoutFeatureAspect aspect = new OMLayoutFeatureAspect();

    JoinPoint joinPoint = mock(JoinPoint.class);
    when(joinPoint.getArgs()).thenReturn(new Object[]{om});
    when(joinPoint.getTarget()).thenReturn(mockOmRequest);

    LambdaTestUtils.intercept(OMException.class,
        "cannot be invoked before finalization",
        () -> aspect.beforeRequestApplyTxn(joinPoint));
  }
}
