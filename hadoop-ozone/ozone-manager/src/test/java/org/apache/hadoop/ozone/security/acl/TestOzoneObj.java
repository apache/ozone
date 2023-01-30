/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Unit test OzoneObjInfo cases.
 */
public class TestOzoneObj {

  private OzoneObjInfo objInfo;
  private OzoneObjInfo.Builder builder;
  private String volume = "vol1";
  private String bucket = "bucket1";
  private String key = "key1";
  private static final OzoneObj.StoreType STORE = OzoneObj.StoreType.OZONE;

  @Test
  public void testGetPathViewer() throws IOException {

    builder = getBuilder(volume, bucket, key);
    objInfo = builder.build();
    assertEquals(objInfo.getVolumeName(), volume);
    assertNotNull("unexpected path accessor",
        objInfo.getOzonePrefixPathViewer());

    objInfo = getBuilder(null, null, null).build();
    assertEquals(objInfo.getVolumeName(), null);
    assertNotNull("unexpected path accessor",
        objInfo.getOzonePrefixPathViewer());

    objInfo = getBuilder(volume, null, null).build();
    assertEquals(objInfo.getVolumeName(), volume);
    assertNotNull("unexpected path accessor",
        objInfo.getOzonePrefixPathViewer());

  }

  private OzoneObjInfo.Builder getBuilder(String withVolume,
      String withBucket, String withKey) throws IOException {

    KeyManager mockKeyManager = mock(KeyManager.class);
    OzonePrefixPath prefixPathViewer = new OzonePrefixPathImpl("vol1",
        "buck1", "file", mockKeyManager);

    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(STORE)
        .setVolumeName(withVolume)
        .setBucketName(withBucket)
        .setKeyName(withKey)
        .setOzonePrefixPath(prefixPathViewer);
  }

}

