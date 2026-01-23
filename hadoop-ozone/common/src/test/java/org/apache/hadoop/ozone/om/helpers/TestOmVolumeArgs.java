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

import static java.util.Collections.emptyList;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Class to test {@link OmVolumeArgs}.
 */
public class TestOmVolumeArgs {

  private static final OzoneAcl USER1_READ = OzoneAcl.of(USER, "user1", ACCESS, READ);
  private static final OzoneAcl USER1_WRITE = OzoneAcl.of(USER, "user1", ACCESS, WRITE);
  private static String user;

  @BeforeAll
  static void setup() throws IOException {
    user = UserGroupInformation.getCurrentUser().getUserName();
  }

  @Test
  public void testClone() {
    OmVolumeArgs subject = createSubject();
    assertSame(subject, subject.copyObject());
  }

  @Test
  void addAcl() {
    OmVolumeArgs omVolumeArgs = createSubject();
    // add user acl to write.
    OmVolumeArgs updated = omVolumeArgs.toBuilder()
        .addAcl(USER1_WRITE)
        .build();

    assertNotEquals(omVolumeArgs.getAcls().get(0), updated.getAcls().get(0));
  }

  @Test
  void setAcls() {
    OmVolumeArgs omVolumeArgs = createSubject();

    // Set user acl to Write_ACL.
    OmVolumeArgs updated = omVolumeArgs.toBuilder()
        .setAcls(Collections.singletonList(USER1_WRITE))
        .build();

    assertEquals(USER1_WRITE, updated.getAcls().get(0));
    assertNotEquals(omVolumeArgs.getAcls().get(0), updated.getAcls().get(0));
  }

  @Test
  void removeAcl() {
    OmVolumeArgs subject = createSubject();

    OmVolumeArgs updated = subject.toBuilder()
        .removeAcl(USER1_READ)
        .build();

    assertEquals(emptyList(), updated.getAcls());
  }

  private static OmVolumeArgs createSubject() {
    return new OmVolumeArgs.Builder()
        .setVolume("vol1")
        .setAdminName("admin")
        .setCreationTime(Time.now())
        .setOwnerName(user)
        .setObjectID(1L)
        .setUpdateID(1L)
        .setQuotaInBytes(Long.MAX_VALUE)
        .addAllMetadata(ImmutableMap.of("key1", "value1", "key2", "value2"))
        .addAcl(USER1_READ)
        .build();
  }
}
