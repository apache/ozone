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

package org.apache.hadoop.ozone.security.acl;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestOzoneAuthorizerFactory {

  private static Stream<Arguments> tmpSharedDirAuthArgs() {
    return Stream.of(
        Arguments.of(true, SharedTmpDirAuthorizer.class),
        Arguments.of(false, MockThirdPartyAuthorizer.class));
  }

  @Test
  void aclsDisabled() {
    // GIVEN
    OzoneManager om = disableAcls();

    // WHEN
    IAccessAuthorizer omAuth =
        OzoneAuthorizerFactory.forOM(om);

    // THEN
    assertSame(OzoneAccessAuthorizer.get(), omAuth);

    assertSameInstanceForSnapshot(om, omAuth);
  }

  @ParameterizedTest
  @ValueSource(classes = {
      OzoneNativeAuthorizer.class,
      MockNativeAuthorizer.class,
  })
  void nativeAuthorizer(Class<? extends IAccessAuthorizer> clazz) {
    // GIVEN
    OzoneManager om = enableAcls(clazz);

    // WHEN
    IAccessAuthorizer omAuth =
        OzoneAuthorizerFactory.forOM(om);

    // THEN
    assertInstanceOf(clazz, omAuth);

    assertNewInstanceForSnapshot(om, omAuth);
  }

  @Test
  void thirdPartyAuthorizer() {
    // GIVEN
    OzoneManager om = enableAcls(MockThirdPartyAuthorizer.class);

    // WHEN
    IAccessAuthorizer omAuth =
        OzoneAuthorizerFactory.forOM(om);

    // THEN
    assertInstanceOf(MockThirdPartyAuthorizer.class, omAuth);

    assertSameInstanceForSnapshot(om, omAuth);
  }

  @ParameterizedTest
  @MethodSource("tmpSharedDirAuthArgs")
  void sharedTmpDirAuthorizer(boolean tmpDirEnabled,
      Class<? extends IAccessAuthorizer> expectedClazz) {
    // GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OZONE_OM_ENABLE_OFS_SHARED_TMP_DIR, tmpDirEnabled);
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS,
        MockThirdPartyAuthorizer.class, IAccessAuthorizer.class);

    OzoneManager om = omMock(conf, true);

    // WHEN
    IAccessAuthorizer omAuth =
        OzoneAuthorizerFactory.forOM(om);

    // THEN
    assertInstanceOf(expectedClazz, omAuth);

    assertSameInstanceForSnapshot(om, omAuth);
  }

  private static void assertSameInstanceForSnapshot(
      OzoneManager om, IAccessAuthorizer omAuth) {
    // GIVEN
    when(om.getAccessAuthorizer()).thenReturn(omAuth);

    // WHEN
    IAccessAuthorizer snapshotAuth =
        OzoneAuthorizerFactory.forSnapshot(om, null, null);

    // THEN
    assertSame(omAuth, snapshotAuth);
  }

  private static void assertNewInstanceForSnapshot(
      OzoneManager om, IAccessAuthorizer omAuth) {
    // GIVEN
    when(om.getAccessAuthorizer()).thenReturn(omAuth);

    // WHEN
    IAccessAuthorizer snapshotAuth =
        OzoneAuthorizerFactory.forSnapshot(om, null, null);

    // THEN
    assertEquals(omAuth.getClass(), snapshotAuth.getClass());
    assertNotSame(omAuth, snapshotAuth);
  }

  @Nonnull
  private static OzoneManager disableAcls() {
    return configureOM(false, OzoneNativeAuthorizer.class);
  }

  @Nonnull
  private static OzoneManager enableAcls(
      Class<? extends IAccessAuthorizer> clazz) {
    return configureOM(true, clazz);
  }

  @Nonnull
  private static OzoneManager configureOM(boolean aclEnabled,
      Class<? extends IAccessAuthorizer> clazz) {

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, aclEnabled);
    conf.setClass(OZONE_ACL_AUTHORIZER_CLASS, clazz, IAccessAuthorizer.class);

    return omMock(conf, aclEnabled);
  }

  private static OzoneManager omMock(OzoneConfiguration conf,
      boolean aclEnabled) {
    OzoneManager om = mock(OzoneManager.class);
    when(om.getConfiguration())
        .thenReturn(conf);
    when(om.getAclsEnabled())
        .thenReturn(aclEnabled);

    return om;
  }

  /**
   * Non-native authorizer for tests.
   */
  public static class MockNativeAuthorizer extends OzoneNativeAuthorizer {
    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return false;
    }
  }

  /**
   * Non-native authorizer for tests.
   */
  public static class MockThirdPartyAuthorizer implements IAccessAuthorizer {
    @Override
    public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context) {
      return false;
    }
  }
}
