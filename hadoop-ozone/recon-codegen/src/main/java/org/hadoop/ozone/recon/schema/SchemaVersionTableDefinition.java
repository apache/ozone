package org.hadoop.ozone.recon.schema;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class for managing the schema of the SchemaVersion table.
 */
@Singleton
public class SchemaVersionTableDefinition implements ReconSchemaDefinition {

  public static final String SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DataSource dataSource;
  private DSLContext dslContext;

  @Inject
  public SchemaVersionTableDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);

    if (!TABLE_EXISTS_CHECK.test(conn, SCHEMA_VERSION_TABLE_NAME)) {
      createSchemaVersionTable();
    }
  }

  @Override
  public void upgradeSchema(String fromVersion, String toVersion) throws SQLException {
    // No schema upgrades needed for the Schema Version table.
  }

  /**
   * Create the Schema Version table.
   */
  private void createSchemaVersionTable() throws SQLException {
    dslContext.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
        .column("version_number", SQLDataType.VARCHAR(10).nullable(false))
        .column("applied_on", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()))
        .execute();
  }
}
