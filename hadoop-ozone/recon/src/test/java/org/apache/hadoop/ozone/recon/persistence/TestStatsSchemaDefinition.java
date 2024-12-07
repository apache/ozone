/**
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
package org.apache.hadoop.ozone.recon.persistence;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hadoop.ozone.recon.schema.StatsSchemaDefinition.GLOBAL_STATS_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

    LocalDateTime now = LocalDateTime.now(Clock.systemUTC().getZone());
    GlobalStats newRecord = new GlobalStats();
    newRecord.setLastUpdatedTimestamp(now);
    newRecord.setKey("key1");
    newRecord.setValue(500L);

    // Create
    dao.insert(newRecord);
    GlobalStats newRecord2 = new GlobalStats();
    LocalDateTime dateTime1 = now.plusSeconds(1);
    newRecord2.setLastUpdatedTimestamp(dateTime1);
    newRecord2.setKey("key2");
    newRecord2.setValue(10L);
    dao.insert(newRecord2);

    // Read
    GlobalStats dbRecord = dao.findById("key1");

    assertEquals("key1", dbRecord.getKey());
    assertEquals(Long.valueOf(500), dbRecord.getValue());
    assertEquals(now, dbRecord.getLastUpdatedTimestamp());

    dbRecord = dao.findById("key2");
    assertEquals("key2", dbRecord.getKey());
    assertEquals(Long.valueOf(10), dbRecord.getValue());
    assertEquals(dateTime1, dbRecord.getLastUpdatedTimestamp());

    // Update
    LocalDateTime dateTime2 = dateTime1.plusSeconds(1);
    dbRecord.setValue(100L);
    dbRecord.setLastUpdatedTimestamp(dateTime2);
    dao.update(dbRecord);

    // Read updated
    dbRecord = dao.findById("key2");

    assertEquals(dateTime2, dbRecord.getLastUpdatedTimestamp());
    assertEquals(Long.valueOf(100L), dbRecord.getValue());

    // Delete
    dao.deleteById("key1");

    // Verify
    dbRecord = dao.findById("key1");

    assertNull(dbRecord);
  }
}
