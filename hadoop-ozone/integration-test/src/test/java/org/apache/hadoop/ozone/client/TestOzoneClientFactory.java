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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Test implementation for OzoneClientFactory.
 */
public class TestOzoneClientFactory {

  @Test
  public void testRemoteException() {

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    Exception e = assertThrows(Exception.class, () -> {
      MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
          .setNumDatanodes(3)
          .build();

      String omPort = cluster.getOzoneManager().getRpcPort();

      UserGroupInformation realUser =
          UserGroupInformation.createRemoteUser("realUser");
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
          "user", realUser);
      proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          conf.set("ozone.security.enabled", "true");
          try (OzoneClient ozoneClient =
                   OzoneClientFactory.getRpcClient("localhost", Integer.parseInt(omPort), conf)) {
            ozoneClient.getObjectStore().listVolumes("/");
          }
          return null;
        }
      });
    });
    assertInstanceOf(AccessControlException.class, e);
  }

}
