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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.jooq.impl.DSL.name;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action to ensure idx_state_container_id exists on UNHEALTHY_CONTAINERS.
 */
@UpgradeActionRecon(feature = ReconLayoutFeature.UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX)
public class UnhealthyContainersStateContainerIdIndexUpgradeAction
    implements ReconUpgradeAction {

  private static final Logger LOG =
      LoggerFactory.getLogger(UnhealthyContainersStateContainerIdIndexUpgradeAction.class);
  private static final String INDEX_NAME = "idx_state_container_id";

  @Override
  public void execute(DataSource source) throws Exception {
    try (Connection conn = source.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        return;
      }

      if (indexExists(conn, INDEX_NAME)) {
        LOG.info("Index {} already exists on {}", INDEX_NAME,
            UNHEALTHY_CONTAINERS_TABLE_NAME);
        return;
      }

      DSLContext dslContext = DSL.using(conn);
      LOG.info("Creating index {} on {}", INDEX_NAME,
          UNHEALTHY_CONTAINERS_TABLE_NAME);
      dslContext.createIndex(INDEX_NAME)
          .on(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME),
              DSL.field(name("container_state")),
              DSL.field(name("container_id")))
          .execute();
    } catch (SQLException e) {
      throw new SQLException("Failed to create " + INDEX_NAME
          + " on " + UNHEALTHY_CONTAINERS_TABLE_NAME, e);
    }
  }

  private boolean indexExists(Connection conn, String indexName)
      throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    try (ResultSet rs = metaData.getIndexInfo(
        null, null, UNHEALTHY_CONTAINERS_TABLE_NAME, false, false)) {
      while (rs.next()) {
        String existing = rs.getString("INDEX_NAME");
        if (existing != null && existing.equalsIgnoreCase(indexName)) {
          return true;
        }
      }
    }
    return false;
  }
}
