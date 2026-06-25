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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link OzoneObjInfo}.
 * */
public class TestOzoneObjInfo {

  private String volume = "vol1";
  private String bucket = "bucket1";
  private String key = "key1";
  private static final OzoneObj.StoreType STORE = OzoneObj.StoreType.OZONE;

  @Test
  public void testGetVolumeName() {

    OzoneObjInfo.Builder builder = getBuilder(volume, bucket, key);
    OzoneObjInfo objInfo = builder.build();
    assertEquals(objInfo.getVolumeName(), volume);

    objInfo = getBuilder(null, null, null).build();
    assertNull(objInfo.getVolumeName());

    objInfo = getBuilder(volume, null, null).build();
    assertEquals(objInfo.getVolumeName(), volume);
  }

  private OzoneObjInfo.Builder getBuilder(String withVolume,
                                          String withBucket,
                                          String withKey) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(ResourceType.VOLUME)
        .setStoreType(STORE)
        .setVolumeName(withVolume)
        .setBucketName(withBucket)
        .setKeyName(withKey);
  }

  @Test
  public void testGetBucketName() {
    OzoneObjInfo objInfo = getBuilder(volume, bucket, key).build();
    assertEquals(objInfo.getBucketName(), bucket);

    objInfo = getBuilder(volume, null, null).build();
    assertNull(objInfo.getBucketName());

    objInfo = getBuilder(null, bucket, null).build();
    assertEquals(objInfo.getBucketName(), bucket);
  }

  @Test
  public void testGetKeyName() {
    OzoneObjInfo objInfo = getBuilder(volume, bucket, key).build();
    assertEquals(objInfo.getKeyName(), key);

    objInfo = getBuilder(volume, null, null).build();
    assertNull(objInfo.getKeyName());

    objInfo = getBuilder(null, bucket, null).build();
    assertNull(objInfo.getKeyName());

    objInfo = getBuilder(null, null, key).build();
    assertEquals(objInfo.getKeyName(), key);
  }

  @Test
  public void testFromProtobufOp() {
    // Key with long path.
    key = "dir1/dir2/dir3/dir4/dir5/abc.txt";
    OzoneManagerProtocolProtos.OzoneObj protoObj = OzoneManagerProtocolProtos.
        OzoneObj.newBuilder()
        .setResType(KEY)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE)
        .setPath(volume + OZONE_URI_DELIMITER +
            bucket + OZONE_URI_DELIMITER + key)
        .build();

    OzoneObjInfo objInfo = OzoneObjInfo.fromProtobuf(protoObj);
    assertEquals(objInfo.getKeyName(), key);
    objInfo = getBuilder(volume, null, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, bucket, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, null, key).build();
    assertEquals(objInfo.getKeyName(), key);

    // Key with long path.
    key = "dir1/dir2/dir3/dir4/dir5/abc.txt";
    protoObj = OzoneManagerProtocolProtos.
        OzoneObj.newBuilder()
        .setResType(KEY)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE)
        .setPath(OZONE_URI_DELIMITER + volume + OZONE_URI_DELIMITER +
            bucket + OZONE_URI_DELIMITER + key)
        .build();

    objInfo = OzoneObjInfo.fromProtobuf(protoObj);
    assertEquals(objInfo.getKeyName(), key);
    objInfo = getBuilder(volume, null, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, bucket, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, null, key).build();
    assertEquals(objInfo.getKeyName(), key);

    // Key with long path.
    key = "dir1/dir2/dir3/dir4/dir5/";
    protoObj = OzoneManagerProtocolProtos.
        OzoneObj.newBuilder()
        .setResType(KEY)
        .setStoreType(OzoneManagerProtocolProtos.OzoneObj.StoreType.OZONE)
        .setPath(OZONE_URI_DELIMITER + volume + OZONE_URI_DELIMITER +
            bucket + OZONE_URI_DELIMITER + key)
        .build();

    objInfo = OzoneObjInfo.fromProtobuf(protoObj);
    assertEquals(objInfo.getKeyName(), key);
    objInfo = getBuilder(volume, null, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, bucket, null).build();
    assertNull(objInfo.getKeyName());
    objInfo = getBuilder(null, null, key).build();
    assertEquals(objInfo.getKeyName(), key);
  }
}
