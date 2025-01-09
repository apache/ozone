/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug.ldb;

import org.apache.hadoop.hdds.cli.DebugSubcommand;

import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Tool that parses rocksdb file.
 */
@CommandLine.Command(
        name = "ldb",
        subcommands = {
            DBScanner.class,
            DropTable.class,
            ListTables.class,
            ValueSchema.class,
        },
        description = "Parse rocksdb file content")
@MetaInfServices(DebugSubcommand.class)
public class RDBParser implements DebugSubcommand {

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  public String getDbPath() {
    return dbPath;
  }

  public void setDbPath(String dbPath) {
    this.dbPath = dbPath;
  }
}
