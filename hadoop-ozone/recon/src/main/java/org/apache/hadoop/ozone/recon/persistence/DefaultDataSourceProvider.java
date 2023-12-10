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

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.jolbox.bonecp.BoneCPDataSource;

/**
 * Provide a {@link javax.sql.DataSource} for the application.
 */
public class DefaultDataSourceProvider implements Provider<DataSource> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDataSourceProvider.class);

  @Inject
  private DataSourceConfiguration configuration;

  /**
   * Create a pooled datasource for the application.
   *
   * Default sqlite database does not work with a connection pool, actually
   * most embedded databases do not, hence returning native implementation for
   * default db.
   */
  @Override
  public DataSource get() {
    String jdbcUrl = configuration.getJdbcUrl();
    LOG.info("JDBC Url for Recon : {} ", jdbcUrl);
    if (StringUtils.contains(jdbcUrl, "derby")) {
      return new DerbyDataSourceProvider(configuration).get();
    } else if (StringUtils.contains(jdbcUrl, "sqlite")) {
      return new SqliteDataSourceProvider(configuration).get();
    }

    BoneCPDataSource cpDataSource = new BoneCPDataSource();
    cpDataSource.setDriverClass(configuration.getDriverClass());
    cpDataSource.setJdbcUrl(configuration.getJdbcUrl());
    cpDataSource.setUsername(configuration.getUserName());
    cpDataSource.setPassword(configuration.getPassword());
    cpDataSource.setDefaultAutoCommit(configuration.setAutoCommit());
    cpDataSource.setConnectionTimeoutInMs(configuration.getConnectionTimeout());
    cpDataSource.setMaxConnectionsPerPartition(
        configuration.getMaxActiveConnections());
    cpDataSource.setMaxConnectionAgeInSeconds(
        configuration.getMaxConnectionAge());
    cpDataSource.setIdleMaxAgeInSeconds(
        configuration.getMaxIdleConnectionAge());
    cpDataSource.setIdleConnectionTestPeriodInSeconds(
        configuration.getIdleConnectionTestPeriod());
    cpDataSource.setConnectionTestStatement(
        configuration.getConnectionTestStatement());

    return cpDataSource;
  }
}
