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

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * The configuration class for the Recon SQL DB.
 */
@ConfigGroup(prefix = "ozone.recon.sql.db")
public class ReconSqlDbConfig {

  @Config(key = "jooq.dialect",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = { ConfigTag.STORAGE, ConfigTag.RECON, ConfigTag.OZONE },
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
