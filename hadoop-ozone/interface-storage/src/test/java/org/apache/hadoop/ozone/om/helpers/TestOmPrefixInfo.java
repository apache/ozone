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

package org.apache.hadoop.ozone.om.helpers;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.junit.jupiter.api.Test;

/**
 * Class to test OmPrefixInfo.
 */
public class TestOmPrefixInfo {

  private static OzoneManagerStorageProtos.OzoneAclInfo buildTestOzoneAclInfo(
      String aclString) {
    OzoneAcl oacl = OzoneAcl.parseAcl(aclString);
    final ByteString rights = oacl.getAclByteString();
    return OzoneManagerStorageProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerStorageProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName(oacl.getName())
        .setRights(rights)
        .setAclScope(OzoneManagerStorageProtos.
            OzoneAclInfo.OzoneAclScope.ACCESS)
        .build();
  }

  private static HddsProtos.KeyValue getDefaultTestMetadata(
      String key, String value) {
    return HddsProtos.KeyValue.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  private static OzoneManagerStorageProtos.PersistedPrefixInfo
      getDefaultTestPrefixInfo(String name, String aclString,
      HddsProtos.KeyValue metadata) {
    return OzoneManagerStorageProtos.PersistedPrefixInfo.newBuilder()
        .setName(name)
        .addAcls(buildTestOzoneAclInfo(aclString))
        .addMetadata(metadata)
        .build();
  }

  private OmPrefixInfo getOmPrefixInfoForTest(String path,
      IAccessAuthorizer.ACLIdentityType identityType,
      String identityString,
      IAccessAuthorizer.ACLType aclType,
      OzoneAcl.AclScope scope) {
    return OmPrefixInfo.newBuilder()
        .setName(path)
        .setAcls(new ArrayList<>(Collections.singletonList(OzoneAcl.of(
            identityType, identityString,
            scope, aclType))))
        .setObjectID(10)
        .setUpdateID(100)
        .build();
  }

  @Test
  public void testCopyObject() {
    String testPath = "/my/custom/path";
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username,
        IAccessAuthorizer.ACLType.WRITE,
        ACCESS);
    OmPrefixInfo clonePrefixInfo = omPrefixInfo.copyObject();

    assertEquals(omPrefixInfo, clonePrefixInfo);

    OmPrefixInfo modifiedPrefixInfo = omPrefixInfo.toBuilder()
        .addAcls(Collections.singletonList(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER, username,
            ACCESS, IAccessAuthorizer.ACLType.READ)))
        .build();

    assertNotEquals(modifiedPrefixInfo, clonePrefixInfo);
  }

  @Test
  public void testImmutability() {
    String testPath = "/my/custom/path";
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username,
        IAccessAuthorizer.ACLType.WRITE,
        ACCESS);

    assertThrows(UnsupportedOperationException.class,
        () -> omPrefixInfo.getAcls().add(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER, username,
            ACCESS, IAccessAuthorizer.ACLType.READ)));
  }

  @Test
  public void testgetFromProtobufOneMetadataOneAcl() {
    String prefixInfoPath = "/mypath/path";
    String aclString = "user:myuser:rw";
    String metakey = "metakey";
    String metaval = "metaval";
    HddsProtos.KeyValue metadata = getDefaultTestMetadata(metakey, metaval);
    OzoneManagerStorageProtos.PersistedPrefixInfo prefixInfo =
        getDefaultTestPrefixInfo(prefixInfoPath,
            aclString, metadata);

    OmPrefixInfo ompri = OmPrefixInfo.getFromProtobuf(prefixInfo);

    assertEquals(prefixInfoPath, ompri.getName());
    assertEquals(1, ompri.getMetadata().size());
    assertEquals(metaval, ompri.getMetadata().get(metakey));
    assertEquals(1, ompri.getAcls().size());
  }

  @Test
  public void testGetProtobuf() {
    String testPath = "/my/custom/path";
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username, IAccessAuthorizer.ACLType.WRITE,
        ACCESS);
    omPrefixInfo = omPrefixInfo.toBuilder()
        .addAllMetadata(singletonMap("key", "value"))
        .build();
    OzoneManagerStorageProtos.PersistedPrefixInfo pi =
        omPrefixInfo.getProtobuf();
    assertEquals(testPath, pi.getName());
    assertEquals(1, pi.getAclsCount());
    assertEquals(1, pi.getMetadataCount());
  }
}
