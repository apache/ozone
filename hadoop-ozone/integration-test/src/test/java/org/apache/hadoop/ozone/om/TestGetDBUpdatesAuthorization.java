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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests authorization of the OzoneManager getDBUpdates RPC (OmClientProtocol DBUpdates).
 * getDBUpdates streams the raw RocksDB delta of the whole OM metadata DB and backs
 * OM->Recon replication, so it is restricted to admins and read-only admins, like the
 * other whole-system reads (listOpenFiles, getQuotaRepairStatus).
 */
public class TestGetDBUpdatesAuthorization {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  private static final String VOL = "vol1";
  private static final String BUCKET = "bucket1";
  private static final String KEY = "key1";
  private static final String RECON_PRINCIPAL = "reconsvc";

  @BeforeAll
  static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.set(OZONE_ADMINISTRATORS, "admin");
    conf.set(OZONE_READONLY_ADMINISTRATORS, RECON_PRINCIPAL);
    // Make admin authorization effective without a KDC so the gate actually runs.
    conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();

    UserGroupInformation.createUserForTesting("admin", new String[] {"admins"})
        .doAs((PrivilegedExceptionAction<Void>) () -> {
          try (OzoneClient c = OzoneClientFactory.getRpcClient(conf)) {
            c.getObjectStore().createVolume(VOL);
            OzoneVolume vol = c.getObjectStore().getVolume(VOL);
            vol.createBucket(BUCKET);
            OzoneBucket b = vol.getBucket(BUCKET);
            byte[] data = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            try (OzoneOutputStream os = b.createKey(KEY, data.length)) {
              os.write(data);
            }
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

  private static DBUpdates getDBUpdates(UserGroupInformation user) throws Exception {
    return user.doAs((PrivilegedExceptionAction<DBUpdates>) () -> {
      OzoneManagerProtocolClientSideTranslatorPB omClient =
          new OzoneManagerProtocolClientSideTranslatorPB(
              OmTransportFactory.create(conf, user, null),
              ClientId.randomId().toString());
      DBUpdatesRequest req = DBUpdatesRequest.newBuilder()
          .setSequenceNumber(0)
          .build();
      return omClient.getDBUpdates(req);
    });
  }

  @Test
  void nonAdminIsDenied() {
    UserGroupInformation nonAdmin =
        UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    OMException ex = assertThrows(OMException.class, () -> getDBUpdates(nonAdmin));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  void adminIsAllowed() throws Exception {
    UserGroupInformation admin =
        UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
    DBUpdates updates = getDBUpdates(admin);
    assertFalse(updates.getData().isEmpty());
  }

  @Test
  void readOnlyAdminIsAllowed() throws Exception {
    UserGroupInformation recon =
        UserGroupInformation.createUserForTesting(RECON_PRINCIPAL, new String[] {"recon"});
    DBUpdates updates = getDBUpdates(recon);
    assertTrue(updates.getData() != null && !updates.getData().isEmpty());
  }
}
