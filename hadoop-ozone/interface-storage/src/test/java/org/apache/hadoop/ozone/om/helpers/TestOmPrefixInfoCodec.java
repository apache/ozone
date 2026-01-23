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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.Proto2CodecTestBase;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OmPrefixInfo#getCodec()}.
 */
public class TestOmPrefixInfoCodec extends Proto2CodecTestBase<OmPrefixInfo> {
  @Override
  public Codec<OmPrefixInfo> getCodec() {
    return OmPrefixInfo.getCodec();
  }

  @Test
  public void testToAndFromPersistedFormat() throws IOException {

    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl ozoneAcl = OzoneAcl.of(ACLIdentityType.USER,
        "hive", ACCESS, ACLType.ALL);
    acls.add(ozoneAcl);
    OmPrefixInfo opiSave = OmPrefixInfo.newBuilder()
        .setName("/user/hive/warehouse")
        .setAcls(acls)
        .addAllMetadata(singletonMap("id", "100"))
        .build();

    final Codec<OmPrefixInfo> codec = getCodec();
    OmPrefixInfo opiLoad = codec.fromPersistedFormat(
        codec.toPersistedFormat(opiSave));

    assertEquals(opiSave, opiLoad, "Loaded not equals to saved");
  }
}
