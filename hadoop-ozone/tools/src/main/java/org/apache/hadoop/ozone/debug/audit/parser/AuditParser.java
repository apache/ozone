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

package org.apache.hadoop.ozone.debug.audit.parser;

import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.debug.audit.parser.handler.LoadCommandHandler;
import org.apache.hadoop.ozone.debug.audit.parser.handler.QueryCommandHandler;
import org.apache.hadoop.ozone.debug.audit.parser.handler.TemplateCommandHandler;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Ozone audit parser tool.
 */
@Command(name = "auditparser",
    description = "Shell parser for Ozone Audit Logs",
    subcommands = {
        LoadCommandHandler.class,
        TemplateCommandHandler.class,
        QueryCommandHandler.class
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
@MetaInfServices(DebugSubcommand.class)
public class AuditParser implements DebugSubcommand {
  /*
  <.db file path> load <file>
  <.db file path> template <template name>
  <.db file path> query <custom sql>
   */
  @Parameters(arity = "1..1", description = "Existing or new .db file.\n" +
      "The database contains only one table called audit defined as:\n" +
      "audit (datetime text, level varchar(7), logger varchar(7), " +
      "user text, ip text, op text, params text, result varchar(7), " +
      "exception text, " +
      "UNIQUE(datetime,level,logger,user,ip,op,params,result))")
  private String database;

  public String getDatabase() {
    return database;
  }
}
