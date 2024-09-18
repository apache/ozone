package org.hadoop.ozone.recon.codegen;

import com.google.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.sql.SQLException;

import static org.jooq.impl.DSL.name;

public class ReconSchemaVersionTableManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReconSchemaVersionTableManager.class);
  public static final String RECON_SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DSLContext dslContext;

  @Inject
  public ReconSchemaVersionTableManager(DataSource dataSource) throws
      SQLException {
    this.dslContext = DSL.using(dataSource.getConnection());
  }

  /**
   * Get the current schema version stored in the RECON_SCHEMA_VERSION_TABLE.
   *
   * @return The current schema version as a String, or null if no entry exists.
   * @throws SQLException if any SQL error occurs.
   */
  public String getCurrentSchemaVersion() {
    return dslContext.select(DSL.field(name("version_number")))
        .from(RECON_SCHEMA_VERSION_TABLE_NAME)
        .fetchOneInto(String.class);  // Return the version number or null if no entry exists
  }

  /**
   * Update the schema version in the RECON_SCHEMA_VERSION_TABLE after all tables are upgraded.
   *
   * @param newVersion The new version to set.
   * @throws SQLException if any SQL error occurs.
   */
  public void updateSchemaVersion(String newVersion) {
    boolean recordExists = dslContext.fetchExists(
        dslContext.selectOne()
            .from(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
    );

    if (recordExists) {
      dslContext.update(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .set(DSL.field(name("version_number")), newVersion)
          .set(DSL.field(name("applied_on")), DSL.currentTimestamp())
          .execute();
      LOG.info("Updated schema version to '{}'.", newVersion);
    } else {
      dslContext.insertInto(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .columns(DSL.field(name("version_number")), DSL.field(name("applied_on")))
          .values(newVersion, DSL.currentTimestamp())
          .execute();
      LOG.info("Inserted new schema version '{}'.", newVersion);
    }
  }

}
