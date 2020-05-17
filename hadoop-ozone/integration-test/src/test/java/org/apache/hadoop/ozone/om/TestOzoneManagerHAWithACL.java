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
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;

/**
 * Test Ozone Manager ACL operation in distributed handler scenario.
 */
public class TestOzoneManagerHAWithACL {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;
  /* Reduce max number of retries to speed up unit test. */
  private static final int OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS = 5;
  private static final int IPC_CLIENT_CONNECT_MAX_RETRIES = 4;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(300_000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    conf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS);
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        IPC_CLIENT_CONNECT_MAX_RETRIES);
    /* Reduce IPC retry interval to speed up unit test. */
    conf.setInt(IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 200);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneBucket setupBucket() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(userName)
        .setAdmin(adminName)
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    Assert.assertTrue(retVolumeinfo.getName().equals(volumeName));
    Assert.assertTrue(retVolumeinfo.getOwner().equals(userName));
    Assert.assertTrue(retVolumeinfo.getAdmin().equals(adminName));

    String bucketName = UUID.randomUUID().toString();
    retVolumeinfo.createBucket(bucketName);

    OzoneBucket ozoneBucket = retVolumeinfo.getBucket(bucketName);

    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));
    Assert.assertTrue(ozoneBucket.getVolumeName().equals(volumeName));

    return ozoneBucket;
  }

  @Test
  public void testAddBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName()).build();

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }
  @Test
  public void testRemoveBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName()).build();

    testRemoveAcl(remoteUserName, ozoneObj, defaultUserAcl);

  }

  @Test
  public void testSetBucketAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName()).build();

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

  @Test
  public void testAddKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setKeyName(key).build();

    testAddAcl(remoteUserName, ozoneObj, userAcl);
  }

  @Test
  public void testRemoveKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setKeyName(key).build();

    testRemoveAcl(remoteUserName, ozoneObj, userAcl);

  }

  @Test
  public void testSetKeyAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    String key = createKey(ozoneBucket);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setKeyName(key).build();

    testSetAcl(remoteUserName, ozoneObj, userAcl);

  }

  @Test
  public void testAddPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) +"/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setPrefixName(prefixName).build();

    testAddAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }
  @Test
  public void testRemovePrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) +"/";
    OzoneAcl userAcl = new OzoneAcl(USER, remoteUserName,
        READ, ACCESS);
    OzoneAcl userAcl1 = new OzoneAcl(USER, "remote",
        READ, ACCESS);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setPrefixName(prefixName).build();

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

  @Test
  public void testSetPrefixAcl() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String remoteUserName = "remoteUser";
    String prefixName = RandomStringUtils.randomAlphabetic(5) +"/";
    OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
        READ, DEFAULT);

    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(ozoneBucket.getVolumeName())
        .setBucketName(ozoneBucket.getName())
        .setPrefixName(prefixName).build();

    testSetAcl(remoteUserName, ozoneObj, defaultUserAcl);
  }


  private void testSetAcl(String remoteUserName, OzoneObj ozoneObj,
      OzoneAcl userAcl) throws Exception {
    // As by default create will add some default acls in RpcClient.

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
      OzoneAcl userAcl)
      throws Exception{
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

  /**
   * Create a key in the bucket.
   * @return the key name.
   */
  static String createKey(OzoneBucket ozoneBucket) throws IOException {
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    String data = "data" + RandomStringUtils.randomNumeric(5);
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(keyName,
        data.length(), ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, new HashMap<>());
    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();
    return keyName;
  }
}
