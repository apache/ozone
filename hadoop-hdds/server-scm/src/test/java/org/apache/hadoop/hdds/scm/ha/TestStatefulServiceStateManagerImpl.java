/*
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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Tests StatefulServiceStateManagerImpl.
 */
public class TestStatefulServiceStateManagerImpl {
  private OzoneConfiguration conf;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;
  private Table<String, ByteString> statefulServiceConfig;
  private StatefulServiceStateManager stateManager;

  @BeforeEach
  public void setup() throws AuthenticationException, IOException {
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTempPath());
    dbStore = DBStoreBuilder.createDBStore(conf, new SCMDBDefinition());
    statefulServiceConfig =
        SCMDBDefinition.STATEFUL_SERVICE_CONFIG.getTable(dbStore);
    scmhaManager = SCMHAManagerStub.getInstance(true, dbStore);
    stateManager =
        StatefulServiceStateManagerImpl.newBuilder()
            .setStatefulServiceConfig(statefulServiceConfig)
            .setRatisServer(scmhaManager.getRatisServer())
            .setSCMDBTransactionBuffer(
                scmhaManager.asSCMHADBTransactionBuffer())
            .build();
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  /**
   * Tests {@link
   * StatefulServiceStateManagerImpl#saveConfiguration(String, ByteString)}.
   * @throws IOException on failure
   */
  @Test
  public void testSaveConfiguration() throws Exception {
    String serviceName = "test";
    String message = "message_string";
    stateManager.saveConfiguration(serviceName,
        ByteString.copyFromUtf8(message));
    scmhaManager.asSCMHADBTransactionBuffer().flush();
    Assertions.assertEquals(ByteString.copyFromUtf8(message),
        stateManager.readConfiguration(serviceName));
  }
}
