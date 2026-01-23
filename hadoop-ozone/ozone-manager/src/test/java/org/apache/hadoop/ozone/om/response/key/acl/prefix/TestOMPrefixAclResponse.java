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

package org.apache.hadoop.ozone.om.response.key.acl.prefix;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.response.key.TestOMKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests TestOMPrefixAclResponse.
 */
public class TestOMPrefixAclResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {
    final OzoneAcl user1 = OzoneAcl.of(USER, "user1",
        ACCESS, ACLType.READ_ACL);
    final OzoneAcl user2 = OzoneAcl.of(USER, "user2",
        ACCESS, ACLType.WRITE);
    final String prefixName = "/vol/buck/prefix/";
    List<OzoneAcl> acls = Arrays.asList(user1, user2);

    OmPrefixInfo omPrefixInfo = OmPrefixInfo.newBuilder()
        .setName(prefixName)
        .setAcls(acls)
        .setUpdateID(1L)
        .setObjectID(ThreadLocalRandom.current().nextLong())
        .build();

    OzoneManagerProtocolProtos.OMResponse setAclResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
            .setSetAclResponse(
                OzoneManagerProtocolProtos.SetAclResponse.newBuilder().setResponse(true).build())
            .build();

    OMPrefixAclResponse prefixAclResponse =
        new OMPrefixAclResponse(setAclResponse, omPrefixInfo);
    prefixAclResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmPrefixInfo persistedPrefixInfo = omMetadataManager.getPrefixTable()
        .getSkipCache(prefixName);
    assertEquals(omPrefixInfo, persistedPrefixInfo);

    String volumeName = "vol";
    String bucketName = "buck";

    OzoneManager ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.resolveBucketLink(Pair.of(volumeName, bucketName)))
        .thenReturn(new ResolvedBucket(volumeName, bucketName, volumeName,
            bucketName, "", BucketLayout.DEFAULT));


    // Verify that in-memory Prefix Tree (Radix Tree) is able to reload from
    // DB successfully
    PrefixManagerImpl prefixManager =
        new PrefixManagerImpl(ozoneManager, omMetadataManager, true);
    OzoneObj prefixObj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName("prefix/")
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    OmPrefixInfo prefixInfo = prefixManager.getPrefixInfo(prefixObj);
    assertEquals(prefixName, prefixInfo.getName());
    assertEquals(1L, prefixInfo.getUpdateID());

    List<OzoneAcl> ozoneAcls = prefixManager.getAcl(prefixObj);
    assertEquals(2, ozoneAcls.size());
    assertEquals(acls, ozoneAcls);

    OzoneManagerProtocolProtos.OMResponse removeAclResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
            .setRemoveAclResponse(
              OzoneManagerProtocolProtos.RemoveAclResponse
                  .newBuilder().setResponse(true).build())
            .build();

    // Remove user2 ACL
    OmPrefixInfo removeOnePrefixInfo = OmPrefixInfo.newBuilder()
        .setName(prefixName)
        .setAcls(Collections.singletonList(user1))
        .setUpdateID(2L)
        .setObjectID(ThreadLocalRandom.current().nextLong())
        .build();

    prefixAclResponse =
        new OMPrefixAclResponse(removeAclResponse, removeOnePrefixInfo);

    prefixAclResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Reload prefix tree from DB and validate again.
    prefixManager =
        new PrefixManagerImpl(ozoneManager, omMetadataManager, true);
    prefixInfo = prefixManager.getPrefixInfo(prefixObj);
    assertEquals(2L, prefixInfo.getUpdateID());

    ozoneAcls = prefixManager.getAcl(prefixObj);
    assertEquals(1, ozoneAcls.size());
    assertEquals(Collections.singletonList(user1), ozoneAcls);

    persistedPrefixInfo = omMetadataManager.getPrefixTable()
        .getSkipCache(prefixName);
    assertEquals(removeOnePrefixInfo, persistedPrefixInfo);

    // Remove all ACL
    OmPrefixInfo removeAllPrefixInfo = OmPrefixInfo.newBuilder()
        .setName(prefixName)
        .setAcls(Collections.emptyList())
        .setUpdateID(3L)
        .setObjectID(ThreadLocalRandom.current().nextLong())
        .build();

    prefixAclResponse =
        new OMPrefixAclResponse(removeAclResponse, removeAllPrefixInfo);

    prefixAclResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNull(omMetadataManager.getPrefixTable()
        .getSkipCache(prefixName));
  }

}
