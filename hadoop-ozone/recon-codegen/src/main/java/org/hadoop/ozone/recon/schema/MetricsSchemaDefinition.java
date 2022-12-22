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
package org.hadoop.ozone.recon.schema;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;

/**
 * Programmatic schema definition of Recon generated metrics
 * at volume, bucket, key, file, directory level.
 */
@Singleton
public class MetricsSchemaDefinition implements ReconSchemaDefinition {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsSchemaDefinition.class);

  private final DataSource dataSource;
  private DSLContext dslContext;

  public static final String VOLUME_METRICS_TABLE_NAME =
      "VOLUME_METRICS";
  public static final String BUCKET_METRICS_TABLE_NAME =
      "BUCKET_METRICS";
  public static final String KEY_METRICS_TABLE_NAME =
      "KEY_METRICS";
  public static final String FILE_METRICS_TABLE_NAME =
      "FILE_METRICS";
  public static final String DIR_METRICS_TABLE_NAME =
      "DIR_METRICS";

  @Inject
  MetricsSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  @Transactional
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    if (!TABLE_EXISTS_CHECK.test(conn, VOLUME_METRICS_TABLE_NAME)) {
      createVolumeMetricsTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, BUCKET_METRICS_TABLE_NAME)) {
      createBucketMetricsTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, KEY_METRICS_TABLE_NAME)) {
      createKeyMetricsTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, FILE_METRICS_TABLE_NAME)) {
      createFileMetricsTable();
    }
    if (!TABLE_EXISTS_CHECK.test(conn, DIR_METRICS_TABLE_NAME)) {
      createDirMetricsTable();
    }
  }

  private void createVolumeMetricsTable() {
    dslContext.createTableIfNotExists(VOLUME_METRICS_TABLE_NAME)
        .column("volume_objectid", SQLDataType.BIGINT.nullable(false))
        .column("bucket_count", SQLDataType.INTEGER.nullable(false))
        .column("access_count", SQLDataType.INTEGER.nullable(false))
        .column("size", SQLDataType.INTEGER.nullable(false))
        .column("creation_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .constraint(DSL.constraint("pk_volume_objectid")
            .primaryKey("volume_objectid"))
        .execute();
  }

  private void createBucketMetricsTable() {
    dslContext.createTableIfNotExists(BUCKET_METRICS_TABLE_NAME)
        .column("bucket_objectid", SQLDataType.BIGINT.nullable(false))
        .column("key_count", SQLDataType.INTEGER.nullable(false))
        .column("access_count", SQLDataType.INTEGER.nullable(false))
        .column("size", SQLDataType.INTEGER.nullable(false))
        .column("creation_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .constraint(DSL.constraint("pk_bucket_objectid")
            .primaryKey("bucket_objectid"))
        .execute();
  }

  private void createKeyMetricsTable() {
    dslContext.createTableIfNotExists(KEY_METRICS_TABLE_NAME)
        .column("key_objectid", SQLDataType.BIGINT.nullable(false))
        .column("access_count", SQLDataType.INTEGER.nullable(false))
        .column("size", SQLDataType.INTEGER.nullable(false))
        .column("creation_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .constraint(DSL.constraint("pk_key_objectid")
            .primaryKey("key_objectid"))
        .execute();
  }

  private void createFileMetricsTable() {
    dslContext.createTableIfNotExists(FILE_METRICS_TABLE_NAME)
        .column("file_objectid", SQLDataType.BIGINT.nullable(false))
        .column("access_count", SQLDataType.INTEGER.nullable(false))
        .column("size", SQLDataType.INTEGER.nullable(false))
        .column("creation_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .constraint(DSL.constraint("pk_file_objectid")
            .primaryKey("file_objectid"))
        .execute();
  }

  private void createDirMetricsTable() {
    dslContext.createTableIfNotExists(DIR_METRICS_TABLE_NAME)
        .column("dir_objectid", SQLDataType.BIGINT.nullable(false))
        .column("access_count", SQLDataType.INTEGER.nullable(false))
        .column("size", SQLDataType.INTEGER.nullable(false))
        .column("creation_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP.nullable(false))
        .constraint(DSL.constraint("pk_dir_objectid")
            .primaryKey("dir_objectid"))
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
