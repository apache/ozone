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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;

/**
 * Class to test OmPrefixInfo.
 */
public class TestOmPrefixInfo {

  String testPath = "/my/custom/path";
  String username = "myuser";


  public OmPrefixInfo getOmPrefixInfoForTest(String path,
      IAccessAuthorizer.ACLIdentityType identityType,
      String identityString,
      IAccessAuthorizer.ACLType aclType,
      OzoneAcl.AclScope scope){
    return new OmPrefixInfo(path,
        Collections.singletonList(new OzoneAcl(
            identityType,identityString,
            aclType, scope)), new HashMap<>(), 10, 100);
  }


  @Test
  public void testCopyObject() {

    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username,
        IAccessAuthorizer.ACLType.WRITE,
        ACCESS);
    OmPrefixInfo clonePrefixInfo = omPrefixInfo.copyObject();

    Assert.assertEquals(omPrefixInfo, clonePrefixInfo);


    // Change acls and check.
    omPrefixInfo.addAcl(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER, username,
        IAccessAuthorizer.ACLType.READ, ACCESS));

    Assert.assertNotEquals(omPrefixInfo, clonePrefixInfo);

  }

  @Test
  public void testgetFromProtobufOneMetadataOneAcl(){

    OmPrefixInfo ompri = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username,
        IAccessAuthorizer.ACLType.READ,
        ACCESS);

    Assert.assertEquals(testPath, ompri.getName() );
    Assert.assertEquals(0, ompri.getMetadata().size());
    Assert.assertEquals(1, ompri.getAcls().size());
  }

  @Test
  public void testGetProtobuf(){

    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(testPath,
        IAccessAuthorizer.ACLIdentityType.USER,
        username, IAccessAuthorizer.ACLType.WRITE,
        ACCESS);
    omPrefixInfo.getMetadata().put("key","value");
    OzoneManagerProtocolProtos.PrefixInfo pi = omPrefixInfo.getProtobuf();
    Assert.assertEquals(testPath,pi.getName());
    Assert.assertEquals(1,pi.getAclsCount());
    Assert.assertEquals(1,pi.getMetadataCount());
  }
}
