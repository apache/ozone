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

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;

/**
 * Class to test OmPrefixInfo.
 */
public class TestOmPrefixInfo {

  @Test
  public void testCopyObject() {
    OmPrefixInfo omPrefixInfo = new OmPrefixInfo("/path",
        Collections.singletonList(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        IAccessAuthorizer.ACLType.WRITE, ACCESS)), new HashMap<>(), 10, 100);

    OmPrefixInfo clonePrefixInfo = omPrefixInfo.copyObject();

    Assert.assertEquals(omPrefixInfo, clonePrefixInfo);


    // Change acls and check.
    omPrefixInfo.addAcl(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        IAccessAuthorizer.ACLType.READ, ACCESS));

    Assert.assertNotEquals(omPrefixInfo, clonePrefixInfo);

  }
}
