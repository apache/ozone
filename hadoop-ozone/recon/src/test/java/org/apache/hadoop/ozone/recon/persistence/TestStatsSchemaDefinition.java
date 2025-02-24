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

import static org.apache.ozone.recon.schema.StatsSchemaDefinition.GLOBAL_STATS_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ozone.recon.schema.generated.tables.daos.GlobalStatsDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.GlobalStats;
import org.junit.jupiter.api.Test;

/**
 * Class used to test StatsSchemaDefinition.
 */
public class TestStatsSchemaDefinition extends AbstractReconSqlDBTest {

  public TestStatsSchemaDefinition() {
    super();
  }

  @Test
  public void testIfStatsSchemaCreated() throws Exception {
    Connection connection = getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        GLOBAL_STATS_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("key", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("value", Types.BIGINT));
    expectedPairs.add(new ImmutablePair<>("last_updated_timestamp",
        Types.TIMESTAMP));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    assertEquals(3, actualPairs.size());
    assertEquals(expectedPairs, actualPairs);
  }

  @Test
  public void testGlobalStatsCRUDOperations() throws Exception {
    Connection connection = getConnection();

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        GLOBAL_STATS_TABLE_NAME, null);

    while (resultSet.next()) {
      assertEquals(GLOBAL_STATS_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    GlobalStatsDao dao = getDao(GlobalStatsDao.class);

    long now = System.currentTimeMillis();
    GlobalStats newRecord = new GlobalStats();
    newRecord.setLastUpdatedTimestamp(new Timestamp(now));
    newRecord.setKey("key1");
    newRecord.setValue(500L);

    // Create
    dao.insert(newRecord);
    GlobalStats newRecord2 = new GlobalStats();
    newRecord2.setLastUpdatedTimestamp(new Timestamp(now + 1000L));
    newRecord2.setKey("key2");
    newRecord2.setValue(10L);
    dao.insert(newRecord2);

    // Read
    GlobalStats dbRecord = dao.findById("key1");

    assertEquals("key1", dbRecord.getKey());
    assertEquals(Long.valueOf(500), dbRecord.getValue());
    assertEquals(new Timestamp(now),
        dbRecord.getLastUpdatedTimestamp());

    dbRecord = dao.findById("key2");
    assertEquals("key2", dbRecord.getKey());
    assertEquals(Long.valueOf(10), dbRecord.getValue());
    assertEquals(new Timestamp(now + 1000L),
        dbRecord.getLastUpdatedTimestamp());

    // Update
    dbRecord.setValue(100L);
    dbRecord.setLastUpdatedTimestamp(new Timestamp(now + 2000L));
    dao.update(dbRecord);

    // Read updated
    dbRecord = dao.findById("key2");

    assertEquals(new Timestamp(now + 2000L),
        dbRecord.getLastUpdatedTimestamp());
    assertEquals(Long.valueOf(100L), dbRecord.getValue());

    // Delete
    dao.deleteById("key1");

    // Verify
    dbRecord = dao.findById("key1");

    assertNull(dbRecord);
  }
}
