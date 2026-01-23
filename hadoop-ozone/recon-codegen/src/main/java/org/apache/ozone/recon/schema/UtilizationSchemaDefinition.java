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

package org.apache.ozone.recon.schema;

import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Programmatic definition of Recon DDL.
 */
@Singleton
public class UtilizationSchemaDefinition implements ReconSchemaDefinition {

  private static final Logger LOG =
      LoggerFactory.getLogger(UtilizationSchemaDefinition.class);

  private final DataSource dataSource;
  private DSLContext dslContext;

  public static final String CLUSTER_GROWTH_DAILY_TABLE_NAME =
      "CLUSTER_GROWTH_DAILY";
  public static final String FILE_COUNT_BY_SIZE_TABLE_NAME =
      "FILE_COUNT_BY_SIZE";
  public static final String CONTAINER_COUNT_BY_SIZE_TABLE_NAME =
      "CONTAINER_COUNT_BY_SIZE";

  @Inject
  UtilizationSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  @Transactional
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    if (!TABLE_EXISTS_CHECK.test(conn, FILE_COUNT_BY_SIZE_TABLE_NAME)) {
      createFileSizeCountTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, CLUSTER_GROWTH_DAILY_TABLE_NAME)) {
      createClusterGrowthTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, CONTAINER_COUNT_BY_SIZE_TABLE_NAME)) {
      createContainerSizeCountTable();
    }
  }

  private void createClusterGrowthTable() {
    dslContext.createTableIfNotExists(CLUSTER_GROWTH_DAILY_TABLE_NAME)
        .column("timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("datanode_id", SQLDataType.INTEGER.nullable(false))
        .column("datanode_host", SQLDataType.VARCHAR(1024))
        .column("rack_id", SQLDataType.VARCHAR(1024))
        .column("available_size", SQLDataType.BIGINT)
        .column("used_size", SQLDataType.BIGINT)
        .column("container_count", SQLDataType.INTEGER)
        .column("block_count", SQLDataType.INTEGER)
        .constraint(DSL.constraint("pk_timestamp_datanode_id")
            .primaryKey("timestamp", "datanode_id"))
        .execute();
  }

  private void createFileSizeCountTable() {
    dslContext.createTableIfNotExists(FILE_COUNT_BY_SIZE_TABLE_NAME)
        .column("volume", SQLDataType.VARCHAR(64).nullable(false))
        .column("bucket", SQLDataType.VARCHAR(64).nullable(false))
        .column("file_size", SQLDataType.BIGINT.nullable(false))
        .column("count", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_volume_bucket_file_size")
            .primaryKey("volume", "bucket", "file_size"))
        .execute();
  }

  private void createContainerSizeCountTable() {
    dslContext.createTableIfNotExists(CONTAINER_COUNT_BY_SIZE_TABLE_NAME)
        .column("container_size", SQLDataType.BIGINT.nullable(false))
        .column("count", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_container_size")
            .primaryKey("container_size"))
        .execute();
  }

  /**
   * Returns the DSL context.
   *
   * @return dslContext
   */
  public DSLContext getDSLContext() {
    return dslContext;
  }
}
