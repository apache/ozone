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

import static org.jooq.impl.DSL.count;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constants and Helper functions for Recon SQL related stuff.
 */
public final class SqlDbUtils {

  public static final String DERBY_DRIVER_CLASS =
      "org.apache.derby.jdbc.EmbeddedDriver";
  public static final String SQLITE_DRIVER_CLASS = "org.sqlite.JDBC";
  public static final String DERBY_DISABLE_LOG_METHOD =
      SqlDbUtils.class.getName() + ".disableDerbyLogFile";

  private static final Logger LOG =
      LoggerFactory.getLogger(SqlDbUtils.class);

  /**
   * Helper function to check if table exists through JOOQ.
   */
  public static final BiPredicate<Connection, String> TABLE_EXISTS_CHECK =
      (conn, tableName) -> {
        try {
          DSL.using(conn).select(count()).from(tableName).execute();
        } catch (DataAccessException ex) {
          LOG.debug(ex.getMessage());
          return false;
        }
        LOG.info("{} table already exists, skipping creation.", tableName);
        return true;
      };

  private SqlDbUtils() {
  }

  /**
   * Create new Derby Database with URL and schema name.
   * @param jdbcUrl JDBC url.
   * @param schemaName Schema name
   * @throws ClassNotFoundException on not finding driver class.
   * @throws SQLException on SQL exception.
   */
  public static void createNewDerbyDatabase(String jdbcUrl, String schemaName)
      throws ClassNotFoundException, SQLException {
    System.setProperty("derby.stream.error.method",
        DERBY_DISABLE_LOG_METHOD);
    Class.forName(DERBY_DRIVER_CLASS);
    try (Connection connection = DriverManager.getConnection(jdbcUrl
        + ";user=" + schemaName
        + ";create=true")) {
      LOG.info("Created derby database at {}.", jdbcUrl);
    }
  }

  /**
   * Used to suppress embedded derby database logging.
   * @return No-Op output stream.
   */
  public static OutputStream disableDerbyLogFile() {
    return new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        // Ignore all log messages
      }
    };
  }

  /**
   * Utility method to list all user-defined tables in the database.
   *
   * @param connection The database connection to use.
   * @return A list of table names (user-defined tables only).
   * @throws SQLException If there is an issue accessing the database metadata.
   */
  public static List<String> listAllTables(Connection connection) throws SQLException {
    List<String> tableNames = new ArrayList<>();
    try (ResultSet resultSet = connection.getMetaData().getTables(null, null, null, new String[]{"TABLE"})) {
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        tableNames.add(tableName);
      }
    }
    LOG.debug("Found {} user-defined tables in the database: {}", tableNames.size(), tableNames);
    return tableNames;
  }
}
