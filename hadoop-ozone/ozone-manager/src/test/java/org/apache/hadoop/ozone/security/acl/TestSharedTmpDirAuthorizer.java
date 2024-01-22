/*
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
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

/**
 * Tests for {@link SharedTmpDirAuthorizer}.
 */
public class TestSharedTmpDirAuthorizer {

  private static OzoneNativeAuthorizer nativeAuthorizer;
  private static IAccessAuthorizer authorizer;
  private static SharedTmpDirAuthorizer sharedTmpDirAuthorizer;

  @BeforeAll
  public static void setUp() {
    nativeAuthorizer = Mockito.mock(OzoneNativeAuthorizer.class);
    authorizer = Mockito.mock(TestOzoneAuthorizerFactory
                                  .MockThirdPartyAuthorizer.class);

    sharedTmpDirAuthorizer =
        new SharedTmpDirAuthorizer(nativeAuthorizer, authorizer);
  }

  private static Stream<Arguments> ozoneObjArgs() {
    return Stream.of(
        Arguments.of("tmp", "tmp", true),
        Arguments.of("tmp", "bucket1", false),
        Arguments.of("vol1", "tmp", false),
        Arguments.of("vol1", "bucket1", false));
  }

  @ParameterizedTest
  @MethodSource("ozoneObjArgs")
  public void testCheckAccess(String volumeName,
      String bucketName, boolean isNative) throws OMException {
    OzoneObjInfo objInfo = OzoneObjInfo.Builder.newBuilder()
                               .setResType(OzoneObj.ResourceType.KEY)
                               .setStoreType(OzoneObj.StoreType.OZONE)
                               .setVolumeName(volumeName)
                               .setBucketName(bucketName)
                               .setKeyName("key1")
                               .build();

    RequestContext context = Mockito.mock(RequestContext.class);
    sharedTmpDirAuthorizer.checkAccess(objInfo, context);

    if (isNative) {
      Mockito.verify(nativeAuthorizer).checkAccess(objInfo, context);
    } else {
      Mockito.verify(authorizer).checkAccess(objInfo, context);
    }
  }
}
