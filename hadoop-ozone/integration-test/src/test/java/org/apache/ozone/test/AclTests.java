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

package org.apache.ozone.test;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests with ACL enabled using Ozone Native Authorizer.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AclTests extends ClusterForTests<MiniOzoneCluster> {

  public static final String ADMIN_USER = "om";
  public static final String ADMIN_GROUP = "ozone";
  public static final UserGroupInformation ADMIN_UGI =
      UserGroupInformation.createUserForTesting(ADMIN_USER, new String[] {ADMIN_GROUP});

  @Override
  protected MiniOzoneCluster.Builder newClusterBuilder() {
    return super.newClusterBuilder()
        .setNumDatanodes(3);
  }

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    loginAdmin();
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    return conf;
  }

  @BeforeEach
  void loginAdmin() {
    UserGroupInformation.setLoginUser(ADMIN_UGI);
  }

  @Nested
  class BucketOwner extends org.apache.hadoop.ozone.om.TestBucketOwner {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneManagerListVolumes extends org.apache.hadoop.ozone.om.TestOzoneManagerListVolumes {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class RecursiveAclWithFSO extends org.apache.hadoop.ozone.om.TestRecursiveAclWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

}
