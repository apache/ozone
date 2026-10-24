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

import static org.sqlite.SQLiteConfig.Pragma.JOURNAL_MODE;

import com.google.inject.Inject;
import com.google.inject.Provider;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

/**
 * Provide a {@link javax.sql.DataSource} for the application.
 */
public class SqliteDataSourceProvider implements Provider<DataSource> {

  /** Same as Derby's default lock wait timeout (derby.locks.waitTimeout). */
  static final int BUSY_TIMEOUT_MS = 60_000;
  private static final Pattern URL_BUSY_TIMEOUT = Pattern.compile("[?&]busy_timeout=(\\d+)", Pattern.CASE_INSENSITIVE);

  private DataSourceConfiguration configuration;

  @Inject
  public SqliteDataSourceProvider(DataSourceConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Create a pooled datasource for the application.
   * <p>
   * Default sqlite database does not work with a connection pool, actually
   * most embedded databases do not, hence returning native implementation for
   * default db.
   * <p>
   * SQLite allows a single writer. WAL lets readers (such as a long unhealthy
   * container export) run without blocking that writer, and the busy timeout
   * makes writers wait for the lock instead of failing with SQLITE_BUSY.
   */
  @Override
  public DataSource get() {
    SQLiteDataSource ds = new SQLiteDataSource();
    String jdbcUrl = configuration.getJdbcUrl();
    ds.setUrl(jdbcUrl);
    // Settings on the data source override the same pragma in the URL, so only set them when the URL does not.
    if (!StringUtils.containsIgnoreCase(jdbcUrl, JOURNAL_MODE.pragmaName)) {
      ds.setJournalMode(SQLiteConfig.JournalMode.WAL.name());
    }
    // SQLiteDataSource ignores busy_timeout in the URL (its config always sets one), so apply the URL value here.
    Matcher urlBusyTimeout = URL_BUSY_TIMEOUT.matcher(jdbcUrl);
    ds.setBusyTimeout(urlBusyTimeout.find() ? Integer.parseInt(urlBusyTimeout.group(1)) : BUSY_TIMEOUT_MS);
    return ds;
  }
}
