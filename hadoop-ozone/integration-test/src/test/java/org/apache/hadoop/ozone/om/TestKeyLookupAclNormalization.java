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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.security.SecurityConfig.OZONE_TEST_AUTHORIZATION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the key READ ACL check and the key read resolve the SAME
 * normalized key name in a FILE_SYSTEM_OPTIMIZED bucket. Because the read path
 * normalizes '.'/'..' path segments before the lookup, the ACL check must operate
 * on the same normalized name; otherwise a per-key ACL could be evaluated against a
 * different (raw, literal) path than the one actually served. These tests assert
 * that a user denied by a per-key ACL is denied whether they request the key
 * directly or via an equivalent un-normalized path, and that an authorized user can
 * still read the key through either form. The same is checked for the read paths
 * that share this resolve-then-read shape (key lookup and object tagging).
 */
public class TestKeyLookupAclNormalization {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  private static final String VOL = "vol1";
  private static final String BUCKET = "buck1";           // FSO (default) layout
  private static final String KEY = "k1";
  private static final String EQUIVALENT_KEY = "a/../k1"; // normalizes to KEY
  private static final byte[] DATA = "some-key-bytes".getBytes(
      java.nio.charset.StandardCharsets.UTF_8);
  private static final Map<String, String> TAGS =
      Collections.singletonMap("t1", "v1");

  private static final UserGroupInformation ADMIN =
      UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
  private static final UserGroupInformation ALICE =
      UserGroupInformation.createUserForTesting("alice", new String[] {"users"});
  private static final UserGroupInformation BOB =
      UserGroupInformation.createUserForTesting("bob", new String[] {"users"});

  @BeforeAll
  static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.set(OZONE_ADMINISTRATORS, "admin");
    // Make authorization (admin + ACL checks) effective without a KDC.
    conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();

    ADMIN.doAs((PrivilegedExceptionAction<Void>) () -> {
      try (OzoneClient c = OzoneClientFactory.getRpcClient(conf)) {
        c.getObjectStore().createVolume(VOL);
        OzoneVolume vol = c.getObjectStore().getVolume(VOL);
        vol.createBucket(BUCKET);
        OzoneBucket bucket = vol.getBucket(BUCKET);
        assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED, bucket.getBucketLayout());

        // alice and bob are ordinary users with READ/LIST on the volume and
        // the bucket. A native KEY read is only granted when the whole
        // volume -> bucket -> key chain grants READ, so both principals need
        // volume- and bucket-level READ before the per-key ACL is decisive.
        vol.addAcl(OzoneAcl.of(ACLIdentityType.USER, "alice",
            OzoneAcl.AclScope.ACCESS, ACLType.READ, ACLType.LIST));
        vol.addAcl(OzoneAcl.of(ACLIdentityType.USER, "bob",
            OzoneAcl.AclScope.ACCESS, ACLType.READ, ACLType.LIST));
        bucket.addAcl(OzoneAcl.of(ACLIdentityType.USER, "alice",
            OzoneAcl.AclScope.ACCESS, ACLType.READ, ACLType.LIST));
        bucket.addAcl(OzoneAcl.of(ACLIdentityType.USER, "bob",
            OzoneAcl.AclScope.ACCESS, ACLType.READ, ACLType.LIST));

        try (OzoneOutputStream os = bucket.createKey(KEY, DATA.length)) {
          os.write(DATA);
        }
        bucket.putObjectTagging(KEY, TAGS);

        // Tighten the per-key ACL: only alice may READ the key; bob is excluded.
        OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
            .setResType(OzoneObj.ResourceType.KEY)
            .setStoreType(OzoneObj.StoreType.OZONE)
            .setVolumeName(VOL).setBucketName(BUCKET).setKeyName(KEY).build();
        c.getObjectStore().setAcl(keyObj, Collections.singletonList(
            OzoneAcl.of(ACLIdentityType.USER, "alice", OzoneAcl.AclScope.ACCESS,
                ACLType.READ, ACLType.ALL)));
      }
      return null;
    });
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private OzoneKeyDetails lookupAs(UserGroupInformation ugi, String keyName)
      throws Exception {
    return ugi.doAs((PrivilegedExceptionAction<OzoneKeyDetails>) () -> {
      try (OzoneClient c = OzoneClientFactory.getRpcClient(conf)) {
        return c.getObjectStore().getVolume(VOL).getBucket(BUCKET).getKey(keyName);
      }
    });
  }

  private Map<String, String> getTagsAs(UserGroupInformation ugi, String keyName)
      throws Exception {
    return ugi.doAs((PrivilegedExceptionAction<Map<String, String>>) () -> {
      try (OzoneClient c = OzoneClientFactory.getRpcClient(conf)) {
        return c.getObjectStore().getVolume(VOL).getBucket(BUCKET)
            .getObjectTagging(keyName);
      }
    });
  }

  /** Control: the tighter per-key ACL denies bob the direct read. */
  @Test
  void deniedUserCannotReadKeyDirectly() {
    OMException ex = assertThrows(OMException.class, () -> lookupAs(BOB, KEY));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  /** A denied user is still denied when requesting an equivalent un-normalized path. */
  @Test
  void deniedUserCannotReadKeyViaEquivalentPath() {
    OMException ex =
        assertThrows(OMException.class, () -> lookupAs(BOB, EQUIVALENT_KEY));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  /** No regression: an authorized user reads the key directly. */
  @Test
  void authorizedUserReadsKeyDirectly() throws Exception {
    OzoneKeyDetails details = lookupAs(ALICE, KEY);
    assertEquals(KEY, details.getName());
    assertEquals(DATA.length, details.getDataSize());
  }

  /** No regression: an authorized user reads the key via an equivalent path. */
  @Test
  void authorizedUserReadsKeyViaEquivalentPath() throws Exception {
    OzoneKeyDetails details = lookupAs(ALICE, EQUIVALENT_KEY);
    assertEquals(KEY, details.getName());
    assertEquals(DATA.length, details.getDataSize());
  }

  /**
   * getObjectTagging resolves the same normalized key as its ACL check: a denied
   * user cannot read the tags via an equivalent un-normalized path.
   */
  @Test
  void deniedUserCannotReadTagsViaEquivalentPath() {
    OMException ex =
        assertThrows(OMException.class, () -> getTagsAs(BOB, EQUIVALENT_KEY));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  /** No regression: an authorized user reads the tags via an equivalent path. */
  @Test
  void authorizedUserReadsTagsViaEquivalentPath() throws Exception {
    assertEquals(TAGS, getTagsAs(ALICE, EQUIVALENT_KEY));
  }
}
