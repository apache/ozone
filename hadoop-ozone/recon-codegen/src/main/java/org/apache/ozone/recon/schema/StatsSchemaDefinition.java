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

/**
 * Class used to create tables that are required for storing Ozone statistics.
 */
@Singleton
public class StatsSchemaDefinition implements ReconSchemaDefinition {

  public static final String GLOBAL_STATS_TABLE_NAME = "GLOBAL_STATS";
  private DSLContext dslContext;
  private final DataSource dataSource;

  @Inject
  StatsSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    if (!TABLE_EXISTS_CHECK.test(conn, GLOBAL_STATS_TABLE_NAME)) {
      createGlobalStatsTable();
    }
  }

  /**
   * Create the Ozone Global Stats table.
   */
  private void createGlobalStatsTable() {
    dslContext.createTableIfNotExists(GLOBAL_STATS_TABLE_NAME)
        .column("key", SQLDataType.VARCHAR(255).nullable(false))
        .column("value", SQLDataType.BIGINT)
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP)
        .constraint(DSL.constraint("pk_key")
            .primaryKey("key"))
        .execute();
  }
}
