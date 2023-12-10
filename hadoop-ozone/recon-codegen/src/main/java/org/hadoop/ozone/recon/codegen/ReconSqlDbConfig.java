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

package org.hadoop.ozone.recon.codegen;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * The configuration class for the Recon SQL DB.
 */
@ConfigGroup(prefix = "ozone.recon.sql.db")
public class ReconSqlDbConfig {

  @Config(key = "driver",
      type = ConfigType.STRING,
      defaultValue = "org.apache.derby.jdbc.EmbeddedDriver",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Recon SQL DB driver class. Defaults to Derby."
  )
  private String driverClass;

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
  }

  @Config(key = "jdbc.url",
      type = ConfigType.STRING,
      defaultValue = "jdbc:derby:${ozone.recon.db.dir}/ozone_recon_derby.db",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Ozone Recon SQL database jdbc url."
  )
  private String jdbcUrl;

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  @Config(key = "username",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Ozone Recon SQL database username."
  )
  private String username;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  @Config(key = "password",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Ozone Recon SQL database password."
  )
  private String password;

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  @Config(key = "auto.commit",
      type = ConfigType.BOOLEAN,
      defaultValue = "true",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Sets the Ozone Recon database connection property of " +
          "auto-commit to true/false."
  )
  private boolean autoCommit;

  public boolean isAutoCommit() {
    return autoCommit;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  @Config(key = "conn.timeout",
      type = ConfigType.TIME,
      defaultValue = "30000ms",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Sets time in milliseconds before call to getConnection " +
          "is timed out."
  )
  private long connectionTimeout;

  public long getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setConnectionTimeout(long connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  @Config(key = "conn.max.active",
      type = ConfigType.INT,
      defaultValue = "5",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "The max active connections to the SQL database."
  )
  private int maxActiveConnections;

  public int getMaxActiveConnections() {
    return maxActiveConnections;
  }

  public void setMaxActiveConnections(int maxActiveConnections) {
    this.maxActiveConnections = maxActiveConnections;
  }

  @Config(key = "conn.max.age",
      type = ConfigType.TIME, timeUnit = SECONDS,
      defaultValue = "1800s",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Sets maximum time a connection can be active in seconds."
  )
  private long connectionMaxAge;

  public long getConnectionMaxAge() {
    return connectionMaxAge;
  }

  public void setConnectionMaxAge(long connectionMaxAge) {
    this.connectionMaxAge = connectionMaxAge;
  }

  @Config(key = "conn.idle.max.age",
      type = ConfigType.TIME, timeUnit = SECONDS,
      defaultValue = "3600s",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Sets maximum time to live for idle connection in seconds."
  )
  private long connectionIdleMaxAge;

  public long getConnectionIdleMaxAge() {
    return connectionIdleMaxAge;
  }

  public void setConnectionIdleMaxAge(long connectionIdleMaxAge) {
    this.connectionIdleMaxAge = connectionIdleMaxAge;
  }

  @Config(key = "conn.idle.test.period",
      type = ConfigType.TIME, timeUnit = SECONDS,
      defaultValue = "60s",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Sets maximum time to live for idle connection in seconds."
  )
  private long connectionIdleTestPeriod;

  public long getConnectionIdleTestPeriod() {
    return connectionIdleTestPeriod;
  }

  public void setConnectionIdleTestPeriod(long connectionIdleTestPeriod) {
    this.connectionIdleTestPeriod = connectionIdleTestPeriod;
  }

  @Config(key = "conn.idle.test",
      type = ConfigType.STRING,
      defaultValue = "SELECT 1",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "The query to send to the DB to maintain keep-alives and " +
          "test for dead connections."
  )
  private String idleTestQuery;

  public String getIdleTestQuery() {
    return idleTestQuery;
  }

  public void setIdleTestQuery(String idleTestQuery) {
    this.idleTestQuery = idleTestQuery;
  }

  @Config(key = "jooq.dialect",
      type = ConfigType.STRING,
      defaultValue = "DERBY",
      tags = {ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE},
      description = "Recon internally uses Jooq to talk to its SQL DB. By " +
          "default, we support Derby and Sqlite out of the box. Please refer " +
          "to https://www.jooq.org/javadoc/latest/org" +
          ".jooq/org/jooq/SQLDialect.html to specify different dialect."
  )
  private String sqlDbDialect;

  public String getSqlDbDialect() {
    return sqlDbDialect;
  }

  public void setSqlDbDialect(String sqlDbDialect) {
    this.sqlDbDialect = sqlDbDialect;
  }

}
