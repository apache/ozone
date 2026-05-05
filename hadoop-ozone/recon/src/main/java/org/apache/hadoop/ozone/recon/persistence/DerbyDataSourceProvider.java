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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.ozone.recon.codegen.JooqCodeGenerator.RECON_SCHEMA_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.createNewDerbyDatabase;

import com.google.inject.Inject;
import com.google.inject.Provider;
import javax.sql.DataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a {@link javax.sql.DataSource} for the application.
 */
public class DerbyDataSourceProvider implements Provider<DataSource> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DerbyDataSourceProvider.class);

  private DataSourceConfiguration configuration;

  @Inject
  DerbyDataSourceProvider(DataSourceConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public DataSource get() {
    String jdbcUrl = configuration.getJdbcUrl();
    try {
      createNewDerbyDatabase(jdbcUrl, RECON_SCHEMA_NAME);
    } catch (Exception e) {
      LOG.error("Error creating Recon Derby DB.", e);
    }
    EmbeddedDataSource dataSource = new EmbeddedDataSource();
    dataSource.setDatabaseName(jdbcUrl.split(":")[2]);
    dataSource.setUser(RECON_SCHEMA_NAME);
    return dataSource;
  }
}
