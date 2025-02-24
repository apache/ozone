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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.ozone.recon.schema.ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.junit.jupiter.api.Test;

/**
 * Class used to test ReconInternalSchemaDefinition.
 */
public class TestReconInternalSchemaDefinition extends AbstractReconSqlDBTest {

  public TestReconInternalSchemaDefinition() {
    super();
  }

  @Test
  public void testSchemaCreated() throws Exception {

    Connection connection = getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        RECON_TASK_STATUS_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("task_name", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("last_updated_timestamp",
        Types.BIGINT));
    expectedPairs.add(new ImmutablePair<>("last_updated_seq_number",
        Types.BIGINT));
    expectedPairs.add(new ImmutablePair<>("last_task_run_status",
        Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("is_current_task_running",
        Types.INTEGER));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(
          resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    assertEquals(5, actualPairs.size());
    assertEquals(expectedPairs, actualPairs);
  }

  @Test
  public void testReconTaskStatusCRUDOperations() throws Exception {
    // Verify table exists
    Connection connection = getConnection();
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        RECON_TASK_STATUS_TABLE_NAME, null);

    while (resultSet.next()) {
      assertEquals(RECON_TASK_STATUS_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    ReconTaskStatusDao dao = getDao(ReconTaskStatusDao.class);
    long now = System.currentTimeMillis();
    ReconTaskStatus newRecord = new ReconTaskStatus();
    newRecord.setTaskName("HelloWorldTask");
    newRecord.setLastUpdatedTimestamp(now);
    newRecord.setLastUpdatedSeqNumber(100L);

    // Create
    dao.insert(newRecord);

    ReconTaskStatus newRecord2 = new ReconTaskStatus();
    newRecord2.setTaskName("GoodbyeWorldTask");
    newRecord2.setLastUpdatedTimestamp(now);
    newRecord2.setLastUpdatedSeqNumber(200L);
    // Create
    dao.insert(newRecord2);

    // Read
    ReconTaskStatus dbRecord = dao.findById("HelloWorldTask");

    assertEquals("HelloWorldTask", dbRecord.getTaskName());
    assertEquals(Long.valueOf(now), dbRecord.getLastUpdatedTimestamp());
    assertEquals(Long.valueOf(100), dbRecord.getLastUpdatedSeqNumber());

    // Update
    dbRecord.setLastUpdatedSeqNumber(150L);
    dao.update(dbRecord);

    // Read updated
    dbRecord = dao.findById("HelloWorldTask");
    assertEquals(Long.valueOf(150), dbRecord.getLastUpdatedSeqNumber());

    // Delete
    dao.deleteById("GoodbyeWorldTask");

    // Verify
    dbRecord = dao.findById("GoodbyeWorldTask");

    assertNull(dbRecord);
  }

}
