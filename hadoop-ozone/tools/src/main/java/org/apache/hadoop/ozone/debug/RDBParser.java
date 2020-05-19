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

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import picocli.CommandLine;

import java.util.HashMap;

/**
 * Tool that parses rocksdb file.
 */
@CommandLine.Command(
        name = "rdbparser",
        description = "Parse rocksdb file content",
        subcommands = {
                SCMDBParser.class,
                ListTables.class
        })
public class RDBParser extends GenericCli {

  @CommandLine.Option(names = {"-path"},
            description = "Database File Path")
    private String dbPath;

  public static HashMap<String, DBColumnFamilyDefinition>
      getColumnFamilyMap() {
    return columnFamilyMap;
  }

  private static HashMap<String, DBColumnFamilyDefinition>  columnFamilyMap;

  public String getDbPath() {
    return dbPath;
  }

  static {
    columnFamilyMap = constructColumnFamilyMap();
  }

  private static HashMap<String, DBColumnFamilyDefinition>
      constructColumnFamilyMap() {
    columnFamilyMap = new HashMap<String, DBColumnFamilyDefinition>();
    columnFamilyMap.put("validCerts", SCMDBDefinition.VALID_CERTS);
    columnFamilyMap.put("deletedBlocks", SCMDBDefinition.DELETED_BLOCKS);
    columnFamilyMap.put("pipelines", SCMDBDefinition.PIPELINES);
    columnFamilyMap.put("revokedCerts", SCMDBDefinition.REVOKED_CERTS);
    columnFamilyMap.put("containers", SCMDBDefinition.CONTAINERS);
    return columnFamilyMap;
  }

  @Override
  public void execute(String[] argv) {
    new RDBParser().run(argv);
  }
}
