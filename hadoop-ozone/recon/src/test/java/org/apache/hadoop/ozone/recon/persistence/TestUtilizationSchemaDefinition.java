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
package org.apache.hadoop.ozone.recon.persistence;

import static org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition.CLUSTER_GROWTH_DAILY_TABLE_NAME;
import static org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition.FILE_COUNT_BY_SIZE_TABLE_NAME;
import static org.hadoop.ozone.recon.schema.tables.ClusterGrowthDailyTable.CLUSTER_GROWTH_DAILY;
import static org.hadoop.ozone.recon.schema.tables.FileCountBySizeTable.FILE_COUNT_BY_SIZE;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ClusterGrowthDailyDao;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ClusterGrowthDaily;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.hadoop.ozone.recon.schema.tables.records.FileCountBySizeRecord;
import org.jooq.Record3;
import org.jooq.Table;
import org.jooq.UniqueKey;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test persistence module provides connection and transaction awareness.
 */
public class TestUtilizationSchemaDefinition extends AbstractReconSqlDBTest {

  @Test
  public void testReconSchemaCreated() throws Exception {
    Connection connection = getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        CLUSTER_GROWTH_DAILY_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("timestamp", Types.TIMESTAMP));
    expectedPairs.add(new ImmutablePair<>("datanode_id", Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("datanode_host", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("rack_id", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("available_size", Types.BIGINT));
    expectedPairs.add(new ImmutablePair<>("used_size", Types.BIGINT));
    expectedPairs.add(new ImmutablePair<>("container_count", Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("block_count", Types.INTEGER));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    Assert.assertEquals(8, actualPairs.size());
    Assert.assertEquals(expectedPairs, actualPairs);

    ResultSet resultSetFileCount = metaData.getColumns(null, null,
        FILE_COUNT_BY_SIZE_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairsFileCount = new ArrayList<>();
    expectedPairsFileCount.add(
        new ImmutablePair<>("volume", Types.VARCHAR));
    expectedPairsFileCount.add(
        new ImmutablePair<>("bucket", Types.VARCHAR));
    expectedPairsFileCount.add(
        new ImmutablePair<>("file_size", Types.BIGINT));
    expectedPairsFileCount.add(
        new ImmutablePair<>("count", Types.BIGINT));

    List<Pair<String, Integer>> actualPairsFileCount = new ArrayList<>();
    while(resultSetFileCount.next()) {
      actualPairsFileCount.add(new ImmutablePair<>(resultSetFileCount.getString(
          "COLUMN_NAME"), resultSetFileCount.getInt(
              "DATA_TYPE")));
    }
    assertEquals("Unexpected number of columns",
        4, actualPairsFileCount.size());
    assertEquals("Columns Do not Match ",
        expectedPairsFileCount, actualPairsFileCount);
  }

  @Test
  public void testClusterGrowthDailyCRUDOperations() throws Exception {
    // Verify table exists
    Connection connection = getConnection();

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        CLUSTER_GROWTH_DAILY_TABLE_NAME, null);

    while (resultSet.next()) {
      Assert.assertEquals(CLUSTER_GROWTH_DAILY_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    ClusterGrowthDailyDao dao = getDao(ClusterGrowthDailyDao.class);
    long now = System.currentTimeMillis();
    ClusterGrowthDaily newRecord = new ClusterGrowthDaily();
    newRecord.setTimestamp(new Timestamp(now));
    newRecord.setDatanodeId(10);
    newRecord.setDatanodeHost("host1");
    newRecord.setRackId("rack1");
    newRecord.setAvailableSize(1024L);
    newRecord.setUsedSize(512L);
    newRecord.setContainerCount(10);
    newRecord.setBlockCount(25);

    // Create
    dao.insert(newRecord);

    // Read
    ClusterGrowthDaily dbRecord =
        dao.findById(getDslContext().newRecord(CLUSTER_GROWTH_DAILY.TIMESTAMP,
            CLUSTER_GROWTH_DAILY.DATANODE_ID)
            .value1(new Timestamp(now)).value2(10));

    Assert.assertEquals("host1", dbRecord.getDatanodeHost());
    Assert.assertEquals("rack1", dbRecord.getRackId());
    Assert.assertEquals(Long.valueOf(1024), dbRecord.getAvailableSize());
    Assert.assertEquals(Long.valueOf(512), dbRecord.getUsedSize());
    Assert.assertEquals(Integer.valueOf(10), dbRecord.getContainerCount());
    Assert.assertEquals(Integer.valueOf(25), dbRecord.getBlockCount());

    // Update
    dbRecord.setUsedSize(700L);
    dbRecord.setBlockCount(30);
    dao.update(dbRecord);

    // Read updated
    dbRecord =
        dao.findById(getDslContext().newRecord(CLUSTER_GROWTH_DAILY.TIMESTAMP,
            CLUSTER_GROWTH_DAILY.DATANODE_ID)
            .value1(new Timestamp(now)).value2(10));

    Assert.assertEquals(Long.valueOf(700), dbRecord.getUsedSize());
    Assert.assertEquals(Integer.valueOf(30), dbRecord.getBlockCount());

    // Delete
    dao.deleteById(getDslContext().newRecord(CLUSTER_GROWTH_DAILY.TIMESTAMP,
        CLUSTER_GROWTH_DAILY.DATANODE_ID)
        .value1(new Timestamp(now)).value2(10));

    // Verify
    dbRecord =
        dao.findById(getDslContext().newRecord(CLUSTER_GROWTH_DAILY.TIMESTAMP,
            CLUSTER_GROWTH_DAILY.DATANODE_ID)
            .value1(new Timestamp(now)).value2(10));

    Assert.assertNull(dbRecord);
  }

  @Test
  public void testFileCountBySizeCRUDOperations() throws SQLException {
    Connection connection = getConnection();

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        FILE_COUNT_BY_SIZE_TABLE_NAME, null);

    while (resultSet.next()) {
      Assert.assertEquals(FILE_COUNT_BY_SIZE_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    FileCountBySizeDao fileCountBySizeDao = getDao(FileCountBySizeDao.class);
    UtilizationSchemaDefinition utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);

    FileCountBySize newRecord = new FileCountBySize();
    newRecord.setVolume("vol1");
    newRecord.setBucket("bucket1");
    newRecord.setFileSize(1024L);
    newRecord.setCount(1L);

    fileCountBySizeDao.insert(newRecord);

    Record3<String, String, Long> recordToFind = utilizationSchemaDefinition
        .getDSLContext().newRecord(FILE_COUNT_BY_SIZE.VOLUME,
            FILE_COUNT_BY_SIZE.BUCKET,
            FILE_COUNT_BY_SIZE.FILE_SIZE)
        .value1("vol1")
        .value2("bucket1")
        .value3(1024L);
    FileCountBySize dbRecord = fileCountBySizeDao.findById(recordToFind);
    assertEquals(Long.valueOf(1), dbRecord.getCount());

    dbRecord.setCount(2L);
    fileCountBySizeDao.update(dbRecord);

    dbRecord = fileCountBySizeDao.findById(recordToFind);
    assertEquals(Long.valueOf(2), dbRecord.getCount());

    Table<FileCountBySizeRecord> fileCountBySizeRecordTable =
        fileCountBySizeDao.getTable();
    List<UniqueKey<FileCountBySizeRecord>> tableKeys =
        fileCountBySizeRecordTable.getKeys();
    for (UniqueKey key : tableKeys) {
      String name = key.getName();
    }
  }
}
