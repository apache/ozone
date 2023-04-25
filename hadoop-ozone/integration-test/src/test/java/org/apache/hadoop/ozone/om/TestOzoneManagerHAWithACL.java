/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.BitSet;
import java.util.Collections;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;

/**
 * Test Ozone Manager ACL operation in distributed handler scenario.
 */
public class TestOzoneManagerHAWithACL extends TestOzoneManagerHA {

  @Test
  public void testRunAllTests() throws Exception {
    testAddBucketAcl();
    testRemoveBucketAcl();
    testSetBucketAcl();

    testAddKeyAcl();
    testRemoveKeyAcl();
    testSetKeyAcl();

    testAddPrefixAcl();
    testRemovePrefixAcl();
    testSetPrefixAcl();

    testLinkBucketAddBucketAcl();
    testLinkBucketRemoveBucketAcl();
    testLinkBucketSetBucketAcl();

    testLinkBucketAddKeyAcl();
    testLinkBucketRemoveKeyAcl();
    testLinkBucketSetKeyAcl();

  }

  public void testAddBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  public void testRemoveBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testRemoveAcl(remoteUserName, ozoneObj, defaultUserAcl);

  }

  public void testSetBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildBucketObj(ozoneBucket);

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  private boolean containsAcl(OzoneAcl ozoneAcl, List<OzoneAcl> ozoneAcls) {
    for (OzoneAcl acl : ozoneAcls) {
      boolean result = compareAcls(ozoneAcl, acl);
      if (result) {
        // We found a match, return.
        return result;
      }
    }
    return false;
  }

  private boolean compareAcls(OzoneAcl givenAcl, OzoneAcl existingAcl) {
    if (givenAcl.getType().equals(existingAcl.getType())
        && givenAcl.getName().equals(existingAcl.getName())
        && givenAcl.getAclScope().equals(existingAcl.getAclScope())) {
      BitSet bitSet = (BitSet) givenAcl.getAclBitSet().clone();
      bitSet.and(existingAcl.getAclBitSet());
      if (bitSet.equals(existingAcl.getAclBitSet())) {
        return true;
      }
    }
    return false;
  }

  public void testAddKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testAddAcl(remoteUserName, ozoneObj, userAcl);
  }

  public void testRemoveKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testRemoveAcl(remoteUserName, ozoneObj, userAcl);

  }

  public void testSetKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = buildKeyObj(ozoneBucket, key);

    testSetAcl(remoteUserName, ozoneObj, userAcl);

  }

  public void testAddPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  public void testRemovePrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, ACCESS);
    OzoneAcl userAcl1 = new OzoneAcl(USER, "remote",
        READ, ACCESS);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    ObjectStore objectStore = getObjectStore();

    boolean result = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(result);

    result = objectStore.addAcl(ozoneObj, userAcl1);
    Assert.assertTrue(result);

    result = objectStore.removeAcl(ozoneObj, userAcl);
    Assert.assertTrue(result);

    // try removing already removed acl.
    result = objectStore.removeAcl(ozoneObj, userAcl);
    Assert.assertFalse(result);

    result = objectStore.removeAcl(ozoneObj, userAcl1);
    Assert.assertTrue(result);

  }

  public void testSetPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) + "/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = buildPrefixObj(ozoneBucket, prefixName);

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }

  public void testLinkBucketAddBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Add ACL to the LINK and verify that it is added to the source bucket
    OzoneAcl acl1 = new OzoneAcl(USER, "remoteUser1", READ, DEFAULT);
    boolean addAcl = getObjectStore().addAcl(linkObj, acl1);
    Assert.assertTrue(addAcl);
    assertEqualsAcls(srcObj, linkObj);

    // Add ACL to the SOURCE and verify that it from link
    OzoneAcl acl2 = new OzoneAcl(USER, "remoteUser2", WRITE, DEFAULT);
    boolean addAcl2 = getObjectStore().addAcl(srcObj, acl2);
    Assert.assertTrue(addAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  public void testLinkBucketRemoveBucketAcl() throws Exception {
    // case1 : test remove link acl
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);
    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls = getObjectStore().getAcl(linkObj);
    Assert.assertTrue(acls.size() > 0);
    // Remove an existing acl.
    boolean removeAcl = getObjectStore().removeAcl(linkObj, acls.get(0));
    Assert.assertTrue(removeAcl);
    assertEqualsAcls(srcObj, linkObj);

    // case2 : test remove src acl
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    OzoneObj linkObj2 = buildBucketObj(linkedBucket2);
    OzoneObj srcObj2 = buildBucketObj(srcBucket2);
    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls2 = getObjectStore().getAcl(srcObj2);
    Assert.assertTrue(acls2.size() > 0);
    // Remove an existing acl.
    boolean removeAcl2 = getObjectStore().removeAcl(srcObj2, acls.get(0));
    Assert.assertTrue(removeAcl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  public void testLinkBucketSetBucketAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);

    OzoneObj linkObj = buildBucketObj(linkedBucket);
    OzoneObj srcObj = buildBucketObj(srcBucket);

    // Set ACL to the LINK and verify that it is set to the source bucket
    List<OzoneAcl> acl1 = Collections.singletonList(
        new OzoneAcl(USER, "remoteUser1", READ, DEFAULT));
    boolean setAcl1 = getObjectStore().setAcl(linkObj, acl1);
    Assert.assertTrue(setAcl1);
    assertEqualsAcls(srcObj, linkObj);

    // Set ACL to the SOURCE and verify that it from link
    List<OzoneAcl> acl2 = Collections.singletonList(
        new OzoneAcl(USER, "remoteUser2", WRITE, DEFAULT));
    boolean setAcl2 = getObjectStore().setAcl(srcObj, acl2);
    Assert.assertTrue(setAcl2);
    assertEqualsAcls(srcObj, linkObj);

  }

  public void testLinkBucketAddKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = new OzoneAcl(USER, user1, READ, DEFAULT);
    testAddAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testAddAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  public void testLinkBucketRemoveKeyAcl() throws Exception {

    // CASE 1: from link bucket
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);
    String user = "remoteUser1";
    OzoneAcl acl = new OzoneAcl(USER, user, READ, DEFAULT);
    testRemoveAcl(user, linkObj, acl);
    assertEqualsAcls(srcObj, linkObj);

    // CASE 2: from src bucket
    OzoneBucket srcBucket2 = setupBucket();
    OzoneBucket linkedBucket2 = linkBucket(srcBucket2);
    String key2 = createKey(srcBucket2);
    OzoneObj linkObj2 = buildKeyObj(linkedBucket2, key2);
    OzoneObj srcObj2 = buildKeyObj(srcBucket2, key2);
    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testRemoveAcl(user2, srcObj2, acl2);
    assertEqualsAcls(srcObj2, linkObj2);

  }

  public void testLinkBucketSetKeyAcl() throws Exception {
    OzoneBucket srcBucket = setupBucket();
    OzoneBucket linkedBucket = linkBucket(srcBucket);
    String key = createKey(linkedBucket);
    OzoneObj linkObj = buildKeyObj(linkedBucket, key);
    OzoneObj srcObj = buildKeyObj(srcBucket, key);

    String user1 = "remoteUser1";
    OzoneAcl acl1 = new OzoneAcl(USER, user1, READ, DEFAULT);
    testSetAcl(user1, linkObj, acl1);  // case1: set link acl
    assertEqualsAcls(srcObj, linkObj);

    String user2 = "remoteUser2";
    OzoneAcl acl2 = new OzoneAcl(USER, user2, READ, DEFAULT);
    testSetAcl(user2, srcObj, acl2);  // case2: set src acl
    assertEqualsAcls(srcObj, linkObj);

  }

  private OzoneObj buildBucketObj(OzoneBucket bucket) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName()).build();
  }

  private OzoneObj buildKeyObj(OzoneBucket bucket, String key) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(key).build();
  }

  private OzoneObj buildPrefixObj(OzoneBucket bucket, String prefix) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setPrefixName(prefix).build();
  }

  private void assertEqualsAcls(OzoneObj srcObj, OzoneObj linkObj)
      throws IOException {
    if (linkObj.getResourceType() == OzoneObj.ResourceType.BUCKET) {
      linkObj = getSourceBucketObj(linkObj);
    }
    Assert.assertEquals(getObjectStore().getAcl(srcObj),
        getObjectStore().getAcl(linkObj));
  }

  private OzoneObj getSourceBucketObj(OzoneObj obj)
      throws IOException {
    assert obj.getResourceType() == OzoneObj.ResourceType.BUCKET;
    OzoneBucket bucket = getObjectStore()
        .getVolume(obj.getVolumeName())
        .getBucket(obj.getBucketName());
    if (!bucket.isLink()) {
      return obj;
    }
    obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucket.getSourceBucket())
        .setVolumeName(bucket.getSourceVolume())
        .setKeyName(obj.getKeyName())
        .setResType(obj.getResourceType())
        .setStoreType(obj.getStoreType())
        .build();
    return getSourceBucketObj(obj);
  }

  private void testSetAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    // As by default create will add some default acls in RpcClient.

    ObjectStore objectStore = getObjectStore();
    if (!ozoneObj.getResourceType().name().equals(
        OzoneObj.ResourceType.PREFIX.name())) {
      List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

      Assert.assertTrue(acls.size() > 0);
    }

    OzoneAcl modifiedUserAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);

    List<OzoneAcl> newAcls = Collections.singletonList(modifiedUserAcl);
    boolean setAcl = objectStore.setAcl(ozoneObj, newAcls);
    Assert.assertTrue(setAcl);

    // Get acls and check whether they are reset or not.
    List<OzoneAcl> getAcls = objectStore.getAcl(ozoneObj);

    Assert.assertTrue(newAcls.size() == getAcls.size());
    int i = 0;
    for (OzoneAcl ozoneAcl : newAcls) {
      Assert.assertTrue(compareAcls(getAcls.get(i++), ozoneAcl));
    }

  }

  private void testAddAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();
    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(addAcl);

    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assert.assertTrue(containsAcl(userAcl, acls));

    // Add an already existing acl.
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertFalse(addAcl);

    // Add an acl by changing acl type with same type, name and scope.
    userAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(addAcl);
  }

  private void testAddLinkAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();
    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(addAcl);

    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assert.assertTrue(containsAcl(userAcl, acls));

    // Add an already existing acl.
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertFalse(addAcl);

    // Add an acl by changing acl type with same type, name and scope.
    userAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(addAcl);
  }

  private void testRemoveAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    ObjectStore objectStore = getObjectStore();

    // As by default create will add some default acls in RpcClient.
    List<OzoneAcl> acls = objectStore.getAcl(ozoneObj);

    Assert.assertTrue(acls.size() > 0);

    // Remove an existing acl.
    boolean removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    Assert.assertTrue(removeAcl);

    // Trying to remove an already removed acl.
    removeAcl = objectStore.removeAcl(ozoneObj, acls.get(0));
    Assert.assertFalse(removeAcl);

    boolean addAcl = objectStore.addAcl(ozoneObj, userAcl);
    Assert.assertTrue(addAcl);

    // Just changed acl type here to write, rest all is same as defaultUserAcl.
    OzoneAcl modifiedUserAcl = new OzoneAcl(USER, remoteUserName,
        WRITE, DEFAULT);
    addAcl = objectStore.addAcl(ozoneObj, modifiedUserAcl);
    Assert.assertTrue(addAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, modifiedUserAcl);
    Assert.assertTrue(removeAcl);

    removeAcl = objectStore.removeAcl(ozoneObj, userAcl);
    Assert.assertTrue(removeAcl);
  }
}
