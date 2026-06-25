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

import static org.apache.ozone.recon.schema.SchemaVersionTableDefinition.SCHEMA_VERSION_TABLE_NAME;
import static org.jooq.impl.DSL.name;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.upgrade.ComponentUpgradeActionProvider;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManager;
import org.apache.hadoop.ozone.upgrade.UpgradeException;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.UpdateSetMoreStep;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon-specific version manager for SQL schema upgrades.
 */
@Singleton
public class ReconVersionManager extends ComponentVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReconVersionManager.class);

  private final DataSource dataSource;
  private final Map<ComponentVersion, ReconUpgradeAction> upgradeActions;

  @Inject
  public ReconVersionManager(DataSource dataSource) throws SQLException {
    this(dataSource, new ReconUpgradeActionProvider());
  }

  @VisibleForTesting
  ReconVersionManager(DataSource dataSource,
      ComponentUpgradeActionProvider<ReconUpgradeAction> upgradeActionProvider) throws SQLException {
    super(loadApparentVersion(dataSource), ReconVersion.SOFTWARE_VERSION);
    this.dataSource = dataSource;
    this.upgradeActions = upgradeActionProvider.load();
  }

  private static ComponentVersion loadApparentVersion(DataSource dataSource) throws SQLException {
    int persisted = readPersistedApparentVersion(dataSource);
    ReconVersion apparent = ReconVersion.deserialize(persisted);
    if (apparent == ReconVersion.UNKNOWN_VERSION) {
      throw new SQLException("Initialization failed. Database contains unknown apparent version "
          + persisted + " for software version " + ReconVersion.SOFTWARE_VERSION
          + ". Make sure this component was not downgraded after finalization");
    }
    return apparent;
  }

  /**
   * Returns the persisted apparent version from {@code RECON_SCHEMA_VERSION}.
   * If the table has no row, returns {@link ReconVersion#INITIAL_VERSION}.
   */
  private static int readPersistedApparentVersion(DataSource dataSource) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      DSLContext dsl = DSL.using(conn);
      return dsl.select(DSL.field(name("version_number")))
          .from(DSL.table(SCHEMA_VERSION_TABLE_NAME))
          .fetchOptional()
          .map(record -> record.get(
              DSL.field(name("version_number"), Integer.class)))
          .orElse(ReconVersion.INITIAL_VERSION.serialize());
    } catch (Exception e) {
      LOG.error("Failed to fetch the persisted apparent version.", e);
      throw new SQLException("Unable to read apparent version from the table.", e);
    }
  }

  @VisibleForTesting
  Map<ComponentVersion, ReconUpgradeAction> getUpgradeActionsForTesting() {
    return upgradeActions;
  }

  @Override
  protected void persistApparentVersion(ComponentVersion newVersion) throws IOException {
    int serializedVersion = newVersion.serialize();
    try (Connection conn = dataSource.getConnection()) {
      DSLContext dsl = DSL.using(conn);
      boolean recordExists = dsl.fetchExists(dsl.selectOne()
          .from(DSL.table(SCHEMA_VERSION_TABLE_NAME)));

      if (recordExists) {
        try (UpdateSetMoreStep<?> update = dsl.update(DSL.table(SCHEMA_VERSION_TABLE_NAME))
            .set(DSL.field(name("version_number")), serializedVersion)
            .set(DSL.field(name("applied_on")), DSL.currentTimestamp())) {
          update.execute();
        }
        LOG.info("Updated apparent version to '{}'.", serializedVersion);
      } else {
        try (InsertSetMoreStep<?> insert = dsl.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
            .set(DSL.field(name("version_number")), serializedVersion)
            .set(DSL.field(name("applied_on")), DSL.currentTimestamp())) {
          insert.execute();
        }
        LOG.info("Inserted new apparent version '{}'.", serializedVersion);
      }
    } catch (SQLException e) {
      throw new IOException("Failed to persist apparent version " + newVersion, e);
    }
  }

  @Override
  public int getPersistedApparentVersion() {
    try {
      return readPersistedApparentVersion(dataSource);
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to read persisted apparent version", e);
    }
  }

  @Override
  protected void runUpgradeAction(ComponentVersion version) throws UpgradeException {
    ReconUpgradeAction action = upgradeActions.get(version);
    if (action == null) {
      return;
    }
    try {
      action.execute(dataSource);
    } catch (Exception e) {
      logAndThrow(e, "Recon upgrade action for version " + version + " failed.",
          UpgradeException.ResultCodes.FINALIZE_UPGRADE_ACTION_FAILED);
    }
  }
}
