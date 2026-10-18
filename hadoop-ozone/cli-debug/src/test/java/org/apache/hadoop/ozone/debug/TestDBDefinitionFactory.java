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

package org.apache.hadoop.ozone.debug;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_REPLICA_HISTORY_DB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaOneDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconReplicaHistoryDBDefinition;
import org.junit.jupiter.api.Test;

/**
 * Simple factory unit test.
 */
public class TestDBDefinitionFactory {

  @Test
  public void testGetReconReplicaHistoryDefinition() {
    DBDefinition definition = DBDefinitionFactory.getDefinition(RECON_REPLICA_HISTORY_DB);
    assertInstanceOf(ReconReplicaHistoryDBDefinition.class, definition);
    assertEquals(RECON_REPLICA_HISTORY_DB, definition.getName());
    assertSame(definition, DBDefinitionFactory.getDefinition(
        Paths.get("/tmp", RECON_REPLICA_HISTORY_DB), new OzoneConfiguration()));

    DBColumnFamilyDefinition<?, ?> history = definition.getColumnFamily("replica_history_v2");
    assertNotNull(history);
    assertEquals(Long.class, history.getKeyType());
    assertEquals(ContainerReplicaHistoryList.class, history.getValueType());

    DBColumnFamilyDefinition<?, ?> metadata = definition.getColumnFamily("metadata");
    assertNotNull(metadata);
    assertEquals(String.class, metadata.getKeyType());
    assertEquals(String.class, metadata.getValueType());
  }

  @Test
  public void testGetDefinition() {
    DBDefinition definition = DBDefinitionFactory.getDefinition(OMDBDefinition.get().getName());
    assertInstanceOf(OMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(SCMDBDefinition.get().getName());
    assertInstanceOf(SCMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(ReconSCMDBDefinition.get().getName());
    assertInstanceOf(ReconSCMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_OM_SNAPSHOT_DB + "_1");
    assertInstanceOf(OMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_CONTAINER_KEY_DB + "_1");
    assertInstanceOf(ReconDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V2");
    final Path dbPath = Paths.get("/tmp/test-container.db");
    final OzoneConfiguration conf = new OzoneConfiguration();
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaTwoDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V1");
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaOneDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V3");
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaThreeDBDefinition.class, definition);
  }
}
