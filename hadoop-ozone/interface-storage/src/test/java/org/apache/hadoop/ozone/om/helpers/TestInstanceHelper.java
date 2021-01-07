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

package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;


/**
 * Test TestInstanceHelper.
 *
 * Utility methods to create test instances of protobuf related classes
 */
public final class TestInstanceHelper {

  private TestInstanceHelper(){
    super();
  }

  public static OzoneManagerStorageProtos.OzoneAclInfo buildTestOzoneAclInfo(
      String aclString){
    OzoneAcl oacl = OzoneAcl.parseAcl(aclString);
    ByteString rights = ByteString.copyFrom(oacl.getAclBitSet().toByteArray());
    return OzoneManagerStorageProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerStorageProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName(oacl.getName())
        .setRights(rights)
        .setAclScope(OzoneManagerStorageProtos.
            OzoneAclInfo.OzoneAclScope.ACCESS)
        .build();
  }

  public static HddsProtos.KeyValue getDefaultTestMetadata(
      String key, String value) {
    return HddsProtos.KeyValue.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  public static OzoneManagerStorageProtos.PersistedPrefixInfo
      getDefaultTestPrefixInfo(String name, String aclString,
      HddsProtos.KeyValue metadata) {
    return OzoneManagerStorageProtos.PersistedPrefixInfo.newBuilder()
        .setName(name)
        .addAcls(buildTestOzoneAclInfo(aclString))
        .addMetadata(metadata)
        .build();
  }
}
