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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Collections;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Class to test {@link OmVolumeArgs}.
 */
public class TestOmVolumeArgs {

  @Test
  public void testClone() throws Exception {
    String volumeName = "vol1";
    String admin = "admin";
    String owner = UserGroupInformation.getCurrentUser().getUserName();
    OmVolumeArgs omVolumeArgs = new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(admin).setCreationTime(Time.now()).setOwnerName(owner)
        .setObjectID(1L).setUpdateID(1L).setQuotaInBytes(Long.MAX_VALUE)
        .addMetadata("key1", "value1").addMetadata("key2", "value2")
        .addOzoneAcls(
            OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.USER, "user1",
                ACCESS, IAccessAuthorizer.ACLType.READ)).build();

    OmVolumeArgs cloneVolumeArgs = omVolumeArgs.copyObject();

    assertEquals(omVolumeArgs, cloneVolumeArgs);

    // add user acl to write.
    omVolumeArgs.addAcl(OzoneAcl.of(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        ACCESS, IAccessAuthorizer.ACLType.WRITE));

    // Now check clone acl
    assertNotEquals(cloneVolumeArgs.getAcls().get(0),
        omVolumeArgs.getAcls().get(0));

    // Set user acl to Write_ACL.
    omVolumeArgs.setAcls(Collections.singletonList(OzoneAcl.of(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        ACCESS, IAccessAuthorizer.ACLType.WRITE_ACL)));

    assertNotEquals(cloneVolumeArgs.getAcls().get(0),
        omVolumeArgs.getAcls().get(0));

    // Now clone and check. It should have same as original acl.
    cloneVolumeArgs = (OmVolumeArgs) omVolumeArgs.copyObject();

    assertEquals(omVolumeArgs, cloneVolumeArgs);
    assertEquals(cloneVolumeArgs.getAcls().get(0),
        omVolumeArgs.getAcls().get(0));

    omVolumeArgs.removeAcl(OzoneAcl.of(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        ACCESS, IAccessAuthorizer.ACLType.WRITE_ACL));

    // Removing acl, in original omVolumeArgs it should have no acls.
    assertEquals(0, omVolumeArgs.getAcls().size());
    assertEquals(1, cloneVolumeArgs.getAcls().size());

  }
}
